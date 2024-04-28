import asyncio
from io import BytesIO
import json
import os
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin
import aiofiles
import aiohttp

from dvuploader.file import File
from dvuploader.utils import build_url

TESTING = bool(os.environ.get("DVUPLOADER_TESTING", False))
MAX_FILE_DISPLAY = int(os.environ.get("DVUPLOADER_MAX_FILE_DISPLAY", 50))
MAX_RETRIES = int(os.environ.get("DVUPLOADER_MAX_RETRIES", 10))

assert isinstance(
    MAX_FILE_DISPLAY, int
), "DVUPLOADER_MAX_FILE_DISPLAY must be an integer"

assert isinstance(MAX_RETRIES, int), "DVUPLOADER_MAX_RETRIES must be an integer"

TICKET_ENDPOINT = "/api/datasets/:persistentId/uploadurls"
ADD_FILE_ENDPOINT = "/api/datasets/:persistentId/addFiles"
UPLOAD_ENDPOINT = "/api/datasets/:persistentId/addFiles?persistentId="
REPLACE_ENDPOINT = "/api/datasets/:persistentId/replaceFiles?persistentId="


async def direct_upload(
    files: List[File],
    dataverse_url: str,
    api_token: str,
    persistent_id: str,
    progress,
    pbars,
    n_parallel_uploads: int,
) -> None:
    """
    Perform parallel direct upload of files to the specified Dataverse repository.

    Args:
        files (List[File]): A list of File objects to be uploaded.
        dataverse_url (str): The URL of the Dataverse repository.
        api_token (str): The API token for the Dataverse repository.
        persistent_id (str): The persistent identifier of the Dataverse dataset.
        progress: The progress object to track the upload progress.
        pbars: A list of progress bars to display the upload progress for each file.
        n_parallel_uploads (int): The number of parallel uploads to perform.

    Returns:
        None
    """

    leave_bar = len(files) < MAX_FILE_DISPLAY
    connector = aiohttp.TCPConnector(limit=n_parallel_uploads)
    async with aiohttp.ClientSession(connector=connector) as session:
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
    connector = aiohttp.TCPConnector(limit=2)
    async with aiohttp.ClientSession(
        headers=headers,
        connector=connector,
    ) as session:
        await _add_files_to_ds(
            session=session,
            files=files,
            dataverse_url=dataverse_url,
            pid=persistent_id,
            progress=progress,
            pbar=pbar,
        )


async def _upload_to_store(
    session: aiohttp.ClientSession,
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
    Uploads a file to a Dataverse collection using direct upload.

    Args:
        session (aiohttp.ClientSession): The aiohttp client session.
        file (File): The file object to upload.
        persistent_id (str): The persistent identifier of the Dataverse dataset to upload to.
        dataverse_url (str): The URL of the Dataverse instance to upload to.
        api_token (str): The API token to use for authentication.
        pbar: The progress bar object.
        progress: The progress object.
        delay (float): The delay in seconds before starting the upload.
        leave_bar (bool): A flag indicating whether to keep the progress bar visible after the upload is complete.

    Returns:
        tuple: A tuple containing the upload status (bool) and the file object.
    """

    await asyncio.sleep(delay)

    ticket = await _request_ticket(
        session=session,
        dataverse_url=dataverse_url,
        api_token=api_token,
        file_size=file._size,
        persistent_id=persistent_id,
    )

    if not "urls" in ticket:
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
    session: aiohttp.ClientSession,
    dataverse_url: str,
    api_token: str,
    persistent_id: str,
    file_size: int,
) -> Dict:
    """Requests a ticket from a Dataverse collection to perform an upload.

    This method will send a request to the Dataverse API to obtain a ticket
    for performing a direct upload to an S3 bucket. The ticket contains a URL
    and storageIdentifier that will be used later to perform the upload.

    Args:
        session (aiohttp.ClientSession): The aiohttp client session to use for the request.
        dataverse_url (str): The URL of the Dataverse installation.
        api_token (str): The API token used to access the dataset.
        persistent_id (str): The persistent identifier of the dataset of interest.
        file_size (int): The size of the file to be uploaded.

    Returns:
        Dict: The response from the Dataverse API, containing the ticket information.
    """
    url = build_url(
        endpoint=urljoin(dataverse_url, TICKET_ENDPOINT),
        key=api_token,
        persistentId=persistent_id,
        size=file_size,
    )

    async with session.get(url) as response:
        response.raise_for_status()
        payload = await response.json()
        return payload["data"]


async def _upload_singlepart(
    session: aiohttp.ClientSession,
    ticket: Dict,
    file: File,
    pbar,
    progress,
    api_token: str,
    leave_bar: bool,
) -> Tuple[bool, str]:
    """
    Uploads a single part of a file to a remote server using HTTP PUT method.

    Args:
        session (aiohttp.ClientSession): The aiohttp client session used for the upload.
        ticket (Dict): A dictionary containing the response from the server.
        filepath (str): The path to the file to be uploaded.
        pbar (tqdm): A progress bar object to track the upload progress.
        progress: The progress object used to update the progress bar.
        leave_bar (bool): A flag indicating whether to keep the progress bar visible after the upload is complete.

    Returns:
        Tuple[bool, str]: A tuple containing the status of the upload (True for success, False for failure)
                          and the storage identifier of the uploaded file.
    """
    assert "url" in ticket, "Couldnt find 'url'"

    if TESTING:
        ticket["url"] = ticket["url"].replace("localstack", "localhost", 1)

    headers = {
        "X-Dataverse-key": api_token,
        "x-amz-tagging": "dv-state=temp",
    }
    storage_identifier = ticket["storageIdentifier"]
    params = {
        "headers": headers,
        "url": ticket["url"],
        "data": file.handler,
    }

    async with session.put(**params) as response:
        status = response.status == 200
        response.raise_for_status()

        if status:
            progress.update(pbar, advance=file._size)
            await asyncio.sleep(0.1)
            progress.update(
                pbar,
                visible=leave_bar,
            )

        return status, storage_identifier


async def _upload_multipart(
    session: aiohttp.ClientSession,
    response: Dict,
    file: File,
    dataverse_url: str,
    pbar,
    progress,
    api_token: str,
):
    """
    Uploads a file to Dataverse using multipart upload.

    Args:
        session (aiohttp.ClientSession): The aiohttp client session.
        response (Dict): The response from the Dataverse API containing the upload ticket information.
        file (File): The file object to be uploaded.
        dataverse_url (str): The URL of the Dataverse instance.
        pbar (tqdm): A progress bar to track the upload progress.
        progress: The progress callback function.

    Returns:
        Tuple[bool, str]: A tuple containing a boolean indicating the success of the upload and the storage identifier for the uploaded file.
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

    return True, storage_identifier


async def _chunked_upload(
    file: File,
    session: aiohttp.ClientSession,
    urls,
    chunk_size: int,
    pbar,
    progress,
):
    """
    Uploads a file in chunks to multiple URLs using the provided session.

    Args:
        file (File): The file object to upload.
        session (aiohttp.ClientSession): The aiohttp client session to use for the upload.
        urls: An iterable of URLs to upload the file chunks to.
        chunk_size (int): The size of each chunk in bytes.
        pbar (tqdm): The progress bar to update during the upload.
        progress: The progress object to track the upload progress.

    Returns:
        List[str]: A list of ETags returned by the server for each uploaded chunk.
    """
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
            )
        )

        progress.update(pbar, advance=len(chunk))

        while chunk:
            chunk = await f.read(chunk_size)
            progress.update(pbar, advance=len(chunk))

            if not chunk:
                break
            else:
                e_tags.append(
                    await _upload_chunk(
                        session=session,
                        url=next(urls),
                        file=BytesIO(chunk),
                    )
                )

    return e_tags


def _validate_ticket_response(response: Dict) -> None:
    """Validate the response from the ticket request to include all necessary fields."""

    assert "abort" in response, "Couldnt find 'abort'"
    assert "complete" in response, "Couldnt find 'complete'"
    assert "partSize" in response, "Couldnt find 'partSize'"
    assert "urls" in response, "Couldnt find 'urls'"
    assert "storageIdentifier" in response, "Could not find 'storageIdentifier'"


async def _upload_chunk(
    session: aiohttp.ClientSession,
    url: str,
    file: BytesIO,
):
    """
    Uploads a chunk of data to the specified URL using the provided session.

    Args:
        session (aiohttp.ClientSession): The session to use for the upload.
        url (str): The URL to upload the chunk to.
        file (ChunkStream): The chunk of data to upload.
        pbar: The progress bar to update during the upload.

    Returns:
        str: The ETag value of the uploaded chunk.
    """

    if TESTING:
        url = url.replace("localstack", "localhost", 1)

    params = {
        "url": url,
        "data": file,
    }

    async with session.put(**params) as response:
        response.raise_for_status()
        return response.headers.get("ETag")


async def _complete_upload(
    session: aiohttp.ClientSession,
    url: str,
    dataverse_url: str,
    e_tags: List[Optional[str]],
    api_token: str,
) -> None:
    """Completes the upload by sending the E tags

    Args:
        session (aiohttp.ClientSession): The aiohttp client session.
        url (str): The URL to send the PUT request to.
        dataverse_url (str): The base URL of the Dataverse instance.
        e_tags (List[str]): The list of E tags to send in the payload.

    Raises:
        aiohttp.ClientResponseError: If the response status code is not successful.
    """

    payload = json.dumps({str(index + 1): e_tag for index, e_tag in enumerate(e_tags)})

    params = {
        "url": urljoin(dataverse_url, url),
        "data": payload,
        "headers": {
            "X-Dataverse-key": api_token,
        },
    }

    async with session.put(**params) as response:
        response.raise_for_status()


async def _abort_upload(
    session: aiohttp.ClientSession,
    url: str,
    dataverse_url: str,
    api_token: str,
):
    """
    Aborts an ongoing upload by sending a DELETE request to the specified URL.

    Args:
        session (aiohttp.ClientSession): The aiohttp client session.
        url (str): The URL to send the DELETE request to.
        dataverse_url (str): The base URL of the Dataverse instance.
        api_token (str): The API token to use for the request.

    Raises:
        aiohttp.ClientResponseError: If the DELETE request fails.
    """

    headers = {
        "X-Dataverse-key": api_token,
    }

    async with session.delete(urljoin(dataverse_url, url), headers=headers) as response:
        response.raise_for_status()


async def _add_files_to_ds(
    session: aiohttp.ClientSession,
    dataverse_url: str,
    pid: str,
    files: List[File],
    progress,
    pbar,
) -> None:
    """
    Adds a file to a Dataverse dataset.

    Args:
        session (aiohttp.ClientSession): The aiohttp client session.
        dataverse_url (str): The URL of the Dataverse instance.
        pid (str): The persistent identifier of the dataset.
        file (File): The file to be added.

    Returns:
        bool: True if the file was added successfully, False otherwise.
    """

    novel_url = urljoin(dataverse_url, UPLOAD_ENDPOINT + pid)
    replace_url = urljoin(dataverse_url, REPLACE_ENDPOINT + pid)

    novel_json_data = _prepare_registration(files, use_replace=False)
    replace_json_data = _prepare_registration(files, use_replace=True)

    await _multipart_json_data_request(
        session=session,
        json_data=novel_json_data,
        url=novel_url,
    )

    await _multipart_json_data_request(
        session=session,
        json_data=replace_json_data,
        url=replace_url,
    )

    progress.update(pbar, advance=1)


def _prepare_registration(files: List[File], use_replace: bool) -> str:
    """
    Prepares the files for registration at the Dataverse instance.

    Args:
        files (List[File]): The list of files to prepare.

    Returns:
        str: A JSON string containing the file data.
    """

    exclude = {"to_replace"} if use_replace else {"to_replace", "file_id"}

    return json.dumps(
        [
            file.model_dump(
                by_alias=True,
                exclude=exclude,
                exclude_none=True,
            )
            for file in files
            if file.to_replace is use_replace
        ],
        indent=2,
    )


async def _multipart_json_data_request(
    json_data: str,
    url: str,
    session: aiohttp.ClientSession,
):
    """
    Sends a multipart/form-data POST request with JSON data to the specified URL using the provided session.

    Args:
        json_data (str): The JSON data to be sent in the request body.
        url (str): The URL to send the request to.
        session (aiohttp.ClientSession): The aiohttp client session to use for the request.

    Raises:
        aiohttp.ClientResponseError: If the response status code is not successful.

    Returns:
        None
    """

    with aiohttp.MultipartWriter("form-data") as writer:
        json_part = writer.append(json_data)
        json_part.set_content_disposition("form-data", name="jsonData")

        async with session.post(url, data=writer) as response:
            response.raise_for_status()
