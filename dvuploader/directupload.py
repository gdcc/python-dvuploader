import asyncio
import json
import os
from io import BytesIO
from typing import AsyncGenerator, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import aiofiles
import httpx
from rich.progress import Progress, TaskID

from dvuploader.file import File
from dvuploader.utils import init_logging, wait_for_dataset_unlock

TESTING = bool(os.environ.get("DVUPLOADER_TESTING", False))
MAX_FILE_DISPLAY = int(os.environ.get("DVUPLOADER_MAX_FILE_DISPLAY", 50))
MAX_RETRIES = int(os.environ.get("DVUPLOADER_MAX_RETRIES", 10))

LOCK_WAIT_TIME = int(os.environ.get("DVUPLOADER_LOCK_WAIT_TIME", 1.5))
LOCK_TIMEOUT = int(os.environ.get("DVUPLOADER_LOCK_TIMEOUT", 300))

assert isinstance(LOCK_WAIT_TIME, int), "DVUPLOADER_LOCK_WAIT_TIME must be an integer"
assert isinstance(LOCK_TIMEOUT, int), "DVUPLOADER_LOCK_TIMEOUT must be an integer"

assert isinstance(MAX_FILE_DISPLAY, int), (
    "DVUPLOADER_MAX_FILE_DISPLAY must be an integer"
)

assert isinstance(MAX_RETRIES, int), "DVUPLOADER_MAX_RETRIES must be an integer"

TICKET_ENDPOINT = "/api/datasets/:persistentId/uploadurls"
ADD_FILE_ENDPOINT = "/api/datasets/:persistentId/addFiles"
UPLOAD_ENDPOINT = "/api/datasets/:persistentId/addFiles?persistentId="
REPLACE_ENDPOINT = "/api/datasets/:persistentId/replaceFiles?persistentId="

# Initialize logging
init_logging()


async def direct_upload(
    files: List[File],
    dataverse_url: str,
    api_token: str,
    persistent_id: str,
    progress,
    pbars,
    n_parallel_uploads: int,
    proxy: Optional[str] = None,
) -> None:
    """
    Perform parallel direct upload of files to the specified Dataverse repository.

    Args:
        files (List[File]): A list of File objects to be uploaded.
        dataverse_url (str): The URL of the Dataverse repository.
        api_token (str): The API token for authentication.
        persistent_id (str): The persistent identifier of the dataset.
        progress: Progress object to track upload progress.
        pbars: List of progress bars for each file.
        n_parallel_uploads (int): Number of concurrent uploads to perform.
        proxy (str): The proxy to use for the upload.
    Returns:
        None
    """

    leave_bar = len(files) < MAX_FILE_DISPLAY

    session_params = {
        "timeout": None,
        "limits": httpx.Limits(max_connections=n_parallel_uploads),
        "proxy": proxy,
        "base_url": dataverse_url,
    }

    async with httpx.AsyncClient(**session_params) as session:
        tasks = [
            _upload_to_store(
                session=session,
                file=file,
                dataverse_url=dataverse_url,
                api_token=api_token,
                persistent_id=persistent_id,
                pbar=pbar,
                progress=progress,
                delay=0.0,
                leave_bar=leave_bar,
            )
            for pbar, file in zip(pbars, files)
        ]

        upload_results = await asyncio.gather(*tasks)

    for status, file in upload_results:
        if status is True:
            continue

        print(f"❌ Failed to upload file '{file.file_name}' to the S3 storage")

    headers = {
        "X-Dataverse-key": api_token,
        "x-amz-tagging": "dv-state=temp",
    }

    pbar = progress.add_task("╰── [bold white]Registering files", total=1)

    session_params = {
        "timeout": None,
        "limits": httpx.Limits(max_connections=n_parallel_uploads),
        "headers": headers,
        "base_url": dataverse_url,
    }

    async with httpx.AsyncClient(**session_params) as session:
        await _add_files_to_ds(
            session=session,
            files=files,
            dataverse_url=dataverse_url,
            pid=persistent_id,
            progress=progress,
            pbar=pbar,
        )


async def _upload_to_store(
    session: httpx.AsyncClient,
    file: File,
    persistent_id: str,
    dataverse_url: str,
    api_token: str,
    pbar,
    progress,
    delay: float,
    leave_bar: bool,
):
    """
    Upload a file to Dataverse storage using direct upload.

    Args:
        session (httpx.AsyncClient): Async HTTP client session.
        file (File): File object to upload.
        persistent_id (str): Dataset persistent identifier.
        dataverse_url (str): Dataverse instance URL.
        api_token (str): API token for authentication.
        pbar: Progress bar for this file.
        progress: Progress tracking object.
        delay (float): Delay before starting upload in seconds.
        leave_bar (bool): Whether to keep progress bar after completion.

    Returns:
        tuple: (success: bool, file: File) indicating upload status and file object.
    """

    await asyncio.sleep(delay)

    ticket = await _request_ticket(
        session=session,
        dataverse_url=dataverse_url,
        api_token=api_token,
        file_size=file._size,
        persistent_id=persistent_id,
    )

    if "urls" not in ticket:
        # Update the progress bar description and append [Singlepart]
        progress.update(
            pbar, description=f"Uploading file '{file.file_name}' [Singlepart]"
        )
        status, storage_identifier = await _upload_singlepart(
            session=session,
            ticket=ticket,
            file=file,
            pbar=pbar,
            progress=progress,
            api_token=api_token,
            leave_bar=leave_bar,
        )

    else:
        # Update the progress bar description and append [Multipart]
        progress.update(
            pbar, description=f"Uploading file '{file.file_name}' [Multipart]"
        )
        status, storage_identifier = await _upload_multipart(
            session=session,
            response=ticket,
            file=file,
            dataverse_url=dataverse_url,
            pbar=pbar,
            progress=progress,
            api_token=api_token,
        )

    file.storageIdentifier = storage_identifier

    return status, file


async def _request_ticket(
    session: httpx.AsyncClient,
    dataverse_url: str,
    api_token: str,
    persistent_id: str,
    file_size: int,
) -> Dict:
    """
    Request an upload ticket from Dataverse.

    Args:
        session (httpx.AsyncClient): Async HTTP client session.
        dataverse_url (str): Dataverse instance URL.
        api_token (str): API token for authentication.
        persistent_id (str): Dataset persistent identifier.
        file_size (int): Size of file to upload in bytes.

    Returns:
        Dict: Upload ticket containing URL and storage identifier.
    """
    url = urljoin(dataverse_url, TICKET_ENDPOINT)

    response = await session.get(
        url,
        timeout=None,
        params={
            "size": file_size,
            "persistentId": persistent_id,
        },
        headers={
            "X-Dataverse-key": api_token,
        },
    )
    response.raise_for_status()

    return response.json()["data"]


async def _upload_singlepart(
    session: httpx.AsyncClient,
    ticket: Dict,
    file: File,
    pbar,
    progress,
    api_token: str,
    leave_bar: bool,
) -> Tuple[bool, str]:
    """
    Upload a file in a single request.

    Args:
        session (httpx.AsyncClient): Async HTTP client session.
        ticket (Dict): Upload ticket from Dataverse.
        file (File): File object to upload.
        pbar: Progress bar for this file.
        progress: Progress tracking object.
        api_token (str): API token for authentication.
        leave_bar (bool): Whether to keep progress bar after completion.

    Returns:
        Tuple[bool, str]: (success status, storage identifier)
    """
    assert "url" in ticket, "Couldn't find 'url'"
    assert file.checksum is not None, "Checksum is required for singlepart uploads"

    if TESTING:
        ticket["url"] = ticket["url"].replace("localstack", "localhost", 1)

    headers = {
        "X-Dataverse-key": api_token,
        "x-amz-tagging": "dv-state=temp",
        "Content-length": str(file._size),
    }

    storage_identifier = ticket["storageIdentifier"]
    params = {
        "headers": headers,
        "url": ticket["url"],
        "content": upload_bytes(
            file=file.get_handler(),  # type: ignore
            progress=progress,
            pbar=pbar,
            hash_func=file.checksum._hash_fun,
        ),
    }

    response = await session.put(**params)
    response.raise_for_status()

    file.apply_checksum()

    if response.status_code == 200:
        progress.update(pbar, advance=file._size)
        await asyncio.sleep(0.1)
        progress.update(
            pbar,
            visible=leave_bar,
        )

    return response.status_code == 200, storage_identifier


async def _upload_multipart(
    session: httpx.AsyncClient,
    response: Dict,
    file: File,
    dataverse_url: str,
    pbar,
    progress,
    api_token: str,
):
    """
    Upload a file using multipart upload.

    Args:
        session (httpx.AsyncClient): Async HTTP client session.
        response (Dict): Upload ticket response from Dataverse.
        file (File): File object to upload.
        dataverse_url (str): Dataverse instance URL.
        pbar: Progress bar for this file.
        progress: Progress tracking object.
        api_token (str): API token for authentication.

    Returns:
        Tuple[bool, str]: (success status, storage identifier)
    """

    _validate_ticket_response(response)

    abort = response["abort"]
    complete = response["complete"]
    part_size = response["partSize"]
    urls = iter(response["urls"].values())
    storage_identifier = response["storageIdentifier"]

    # Chunk file and retrieve paths and urls
    chunk_size = int(part_size)

    try:
        e_tags = await _chunked_upload(
            file=file,
            session=session,
            urls=urls,
            chunk_size=chunk_size,
            pbar=pbar,
            progress=progress,
        )
    except Exception as e:
        print(f"❌ Failed to upload file '{file.file_name}' to the S3 storage")
        await _abort_upload(
            session=session,
            url=abort,
            dataverse_url=dataverse_url,
            api_token=api_token,
        )
        raise e

    await _complete_upload(
        session=session,
        url=complete,
        dataverse_url=dataverse_url,
        e_tags=e_tags,
        api_token=api_token,
    )

    file.apply_checksum()

    return True, storage_identifier


async def _chunked_upload(
    file: File,
    session: httpx.AsyncClient,
    urls,
    chunk_size: int,
    pbar,
    progress,
):
    """
    Upload a file in chunks.

    Args:
        file (File): File object to upload.
        session (httpx.AsyncClient): Async HTTP client session.
        urls: Iterator of upload URLs for each chunk.
        chunk_size (int): Size of each chunk in bytes.
        pbar: Progress bar for this file.
        progress: Progress tracking object.

    Returns:
        List[str]: ETags returned by server for each chunk.
    """
    assert file.checksum is not None, "Checksum is required for multipart uploads"

    e_tags = []

    if not os.path.exists(file.filepath):
        raise NotImplementedError(
            """

            Multipart chunked upload is currently only supported for local files and no in-memory objects.
            Please save the handlers content to a local file and try again.
            """
        )

    async with aiofiles.open(file.filepath, "rb") as f:
        chunk = await f.read(chunk_size)
        e_tags.append(
            await _upload_chunk(
                session=session,
                url=next(urls),
                file=BytesIO(chunk),
                progress=progress,
                pbar=pbar,
                hash_func=file.checksum._hash_fun,
            )
        )

        while chunk:
            chunk = await f.read(chunk_size)

            if not chunk:
                break
            else:
                e_tags.append(
                    await _upload_chunk(
                        session=session,
                        url=next(urls),
                        file=BytesIO(chunk),
                        progress=progress,
                        pbar=pbar,
                        hash_func=file.checksum._hash_fun,
                    )
                )

    return e_tags


def _validate_ticket_response(response: Dict) -> None:
    """
    Validate that upload ticket response contains required fields.

    Args:
        response (Dict): Upload ticket response to validate.

    Raises:
        AssertionError: If required fields are missing.
    """

    assert "abort" in response, "Couldn't find 'abort'"
    assert "complete" in response, "Couldn't find 'complete'"
    assert "partSize" in response, "Couldn't find 'partSize'"
    assert "urls" in response, "Couldn't find 'urls'"
    assert "storageIdentifier" in response, "Could not find 'storageIdentifier'"


async def _upload_chunk(
    session: httpx.AsyncClient,
    url: str,
    file: BytesIO,
    progress: Progress,
    pbar: TaskID,
    hash_func,
):
    """
    Upload a single chunk of data.

    Args:
        session (httpx.AsyncClient): Async HTTP client session.
        url (str): URL to upload chunk to.
        file (BytesIO): Chunk data to upload.
        progress (Progress): Progress tracking object.
        pbar (TaskID): Progress bar task ID.
        hash_func: Hash function for checksum.

    Returns:
        str: ETag from server response.
    """

    if TESTING:
        url = url.replace("localstack", "localhost", 1)

    headers = {
        "Content-length": str(len(file.getvalue())),
    }

    params = {
        "headers": headers,
        "url": url,
        "data": upload_bytes(
            file=file,
            progress=progress,
            pbar=pbar,
            hash_func=hash_func,
        ),
    }

    response = await session.put(**params)
    response.raise_for_status()

    return response.headers.get("ETag")


async def _complete_upload(
    session: httpx.AsyncClient,
    url: str,
    dataverse_url: str,
    e_tags: List[Optional[str]],
    api_token: str,
) -> None:
    """
    Complete a multipart upload by sending ETags.

    Args:
        session (httpx.AsyncClient): Async HTTP client session.
        url (str): URL to send completion request to.
        dataverse_url (str): Dataverse instance URL.
        e_tags (List[str]): List of ETags from uploaded chunks.
        api_token (str): API token for authentication.
    """

    payload = json.dumps({str(index + 1): e_tag for index, e_tag in enumerate(e_tags)})

    params = {
        "url": urljoin(dataverse_url, url),
        "data": payload,
        "headers": {
            "X-Dataverse-key": api_token,
        },
    }

    response = await session.put(**params)
    response.raise_for_status()


async def _abort_upload(
    session: httpx.AsyncClient,
    url: str,
    dataverse_url: str,
    api_token: str,
):
    """
    Abort an in-progress multipart upload.

    Args:
        session (httpx.AsyncClient): Async HTTP client session.
        url (str): URL to send abort request to.
        dataverse_url (str): Dataverse instance URL.
        api_token (str): API token for authentication.
    """

    headers = {"X-Dataverse-key": api_token}

    url = urljoin(dataverse_url, url)
    response = await session.delete(url, headers=headers)
    response.raise_for_status()


async def _add_files_to_ds(
    session: httpx.AsyncClient,
    dataverse_url: str,
    pid: str,
    files: List[File],
    progress,
    pbar,
) -> None:
    """
    Register uploaded files with the dataset.

    Args:
        session (httpx.AsyncClient): Async HTTP client session.
        dataverse_url (str): Dataverse instance URL.
        pid (str): Dataset persistent identifier.
        files (List[File]): List of uploaded files to register.
        progress: Progress tracking object.
        pbar: Progress bar for registration.
    """

    await wait_for_dataset_unlock(
        session=session,
        persistent_id=pid,
        sleep_time=LOCK_WAIT_TIME,
        timeout=LOCK_TIMEOUT,
    )

    novel_url = urljoin(dataverse_url, UPLOAD_ENDPOINT + pid)
    replace_url = urljoin(dataverse_url, REPLACE_ENDPOINT + pid)

    novel_json_data = _prepare_registration(files, use_replace=False)
    replace_json_data = _prepare_registration(files, use_replace=True)

    if novel_json_data:
        # Register new files, if any
        await _multipart_json_data_request(
            session=session,
            json_data=novel_json_data,
            url=novel_url,
        )

    if replace_json_data:
        # Register replacement files, if any
        await _multipart_json_data_request(
            session=session,
            json_data=replace_json_data,
            url=replace_url,
        )

    progress.update(pbar, advance=1)


def _prepare_registration(files: List[File], use_replace: bool) -> List[Dict]:
    """
    Prepare file metadata for registration.

    Args:
        files (List[File]): List of files to prepare metadata for.
        use_replace (bool): Whether these are replacement files.

    Returns:
        List[Dict]: List of file metadata dictionaries.
    """

    exclude = {"to_replace"} if use_replace else {"to_replace", "file_id"}

    return [
        file.model_dump(
            by_alias=True,
            exclude=exclude,
            exclude_none=True,
        )
        for file in files
        if file.to_replace is use_replace
    ]


async def _multipart_json_data_request(
    json_data: List[Dict],
    url: str,
    session: httpx.AsyncClient,
):
    """
    Send multipart form request with JSON data.

    Args:
        json_data (List[Dict]): JSON data to send.
        url (str): URL to send request to.
        session (httpx.AsyncClient): Async HTTP client session.

    Raises:
        httpx.HTTPStatusError: If request fails.
    """

    files = {
        "jsonData": (
            None,
            BytesIO(json.dumps(json_data).encode()),
            "application/json",
        ),
    }

    response = await session.post(url, files=files)

    if not response.is_success:
        raise httpx.HTTPStatusError(
            f"Failed to register files: {response.text}",
            request=response.request,
            response=response,
        )


async def upload_bytes(
    file: BytesIO,
    progress: Progress,
    pbar: TaskID,
    hash_func,
) -> AsyncGenerator[bytes, None]:
    """
    Generate chunks of file data for upload.

    Args:
        file (BytesIO): File to read chunks from.
        progress (Progress): Progress tracking object.
        pbar (TaskID): Progress bar task ID.
        hash_func: Hash function for checksum.

    Yields:
        bytes: Next chunk of file data.
    """
    while True:
        data = file.read(1024 * 1024)  # 1MB

        if not data:
            break

        # Update the hash function with the data
        hash_func.update(data)

        # Update the progress bar
        progress.update(pbar, advance=len(data))

        yield data
