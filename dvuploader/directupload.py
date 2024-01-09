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

TICKET_ENDPOINT = "/api/datasets/:persistentId/uploadurls"
ADD_FILE_ENDPOINT = "/api/datasets/:persistentId/addFiles"
UPLOAD_ENDPOINT = "/api/datasets/:persistentId/add?persistentId="
REPLACE_ENDPOINT = "/api/files/{FILE_ID}/replace"


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

    headers = {
        "X-Dataverse-key": api_token,
    }
    params = {
        "headers": headers,
        "connector": aiohttp.TCPConnector(limit=n_parallel_uploads),
    }
    async with aiohttp.ClientSession(**params) as session:
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
            )
            for pbar, file in zip(pbars, files)
        ]

        upload_results = await asyncio.gather(*tasks)

    for status, file in upload_results:
        if status is True:
            continue

        print(f"❌ Failed to upload file '{file.fileName}' to the S3 storage")

    connector = aiohttp.TCPConnector(limit=4)
    pbar = progress.add_task("╰── [bold white]Registering files", total=len(files))
    results = []
    async with aiohttp.ClientSession(
        headers=headers,
        connector=connector,
    ) as session:
        for file in files:
            results.append(
                await _add_file_to_ds(
                    session=session,
                    file=file,
                    dataverse_url=dataverse_url,
                    pid=persistent_id,
                )
            )

            progress.update(pbar, advance=1)

    for file, status in zip(files, results):
        if status is False:
            print(f"❌ Failed to register file '{file.fileName}' at Dataverse")


async def _upload_to_store(
    session: aiohttp.ClientSession,
    file: File,
    persistent_id: str,
    dataverse_url: str,
    api_token: str,
    pbar,
    progress,
    delay: float,
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

    Returns:
        tuple: A tuple containing the upload status (bool) and the file object.
    """

    await asyncio.sleep(delay)

    assert file.fileName is not None, "File name is None"
    assert os.path.exists(file.filepath), f"File {file.filepath} does not exist"

    file_size = os.path.getsize(file.filepath)
    ticket = await _request_ticket(
        session=session,
        dataverse_url=dataverse_url,
        api_token=api_token,
        file_size=file_size,
        persistent_id=persistent_id,
    )

    if not "urls" in ticket:
        status, storage_identifier = await _upload_singlepart(
            session=session,
            ticket=ticket,
            filepath=file.filepath,
            pbar=pbar,
            progress=progress,
        )

    else:
        status, storage_identifier = await _upload_multipart(
            session=session,
            response=ticket,
            filepath=file.filepath,
            dataverse_url=dataverse_url,
            pbar=pbar,
            progress=progress,
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
    filepath: str,
    pbar,
    progress,
) -> Tuple[bool, str]:
    """
    Uploads a single part of a file to a remote server using HTTP PUT method.

    Args:
        session (aiohttp.ClientSession): The aiohttp client session used for the upload.
        ticket (Dict): A dictionary containing the response from the server.
        filepath (str): The path to the file to be uploaded.
        pbar (tqdm): A progress bar object to track the upload progress.
        progress: The progress object used to update the progress bar.

    Returns:
        Tuple[bool, str]: A tuple containing the status of the upload (True for success, False for failure)
                          and the storage identifier of the uploaded file.
    """
    assert "url" in ticket, "Couldnt find 'url'"

    if TESTING:
        ticket["url"] = ticket["url"].replace("localstack", "localhost", 1)

    storage_identifier = ticket["storageIdentifier"]
    params = {
        "url": ticket["url"],
        "data": open(filepath, "rb"),
    }

    async with session.put(**params) as response:
        status = response.status == 200

        if status:
            progress.update(pbar, advance=os.path.getsize(filepath))

        return status, storage_identifier


async def _upload_multipart(
    session: aiohttp.ClientSession,
    response: Dict,
    filepath: str,
    dataverse_url: str,
    pbar,
    progress,
):
    """
    Uploads a file to Dataverse using multipart upload.

    Args:
        session (aiohttp.ClientSession): The aiohttp client session.
        response (Dict): The response from the Dataverse API containing the upload ticket information.
        filepath (str): The path to the file to be uploaded.
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
            filepath=filepath,
            session=session,
            urls=urls,
            chunk_size=chunk_size,
            pbar=pbar,
            progress=progress,
        )
    except Exception as e:
        print(f"❌ Failed to upload file '{filepath}' to the S3 storage")
        await _abort_upload(
            session=session,
            url=abort,
            dataverse_url=dataverse_url,
        )
        raise e

    await _complete_upload(
        session=session,
        url=complete,
        dataverse_url=dataverse_url,
        e_tags=e_tags,
    )

    return True, storage_identifier


async def _chunked_upload(
    filepath: str,
    session: aiohttp.ClientSession,
    urls,
    chunk_size: int,
    pbar,
    progress,
):
    """
    Uploads a file in chunks to multiple URLs using the provided session.

    Args:
        filepath (str): The path of the file to upload.
        session (aiohttp.ClientSession): The aiohttp client session to use for the upload.
        urls: An iterable of URLs to upload the file chunks to.
        chunk_size (int): The size of each chunk in bytes.
        pbar (tqdm): The progress bar to update during the upload.
        progress: The progress object to track the upload progress.

    Returns:
        List[str]: A list of ETags returned by the server for each uploaded chunk.
    """
    e_tags = []
    async with aiofiles.open(filepath, "rb") as f:
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

    async with session.put(urljoin(dataverse_url, url), data=payload) as response:
        response.raise_for_status()


async def _abort_upload(
    session: aiohttp.ClientSession,
    url: str,
    dataverse_url: str,
):
    """
    Aborts an ongoing upload by sending a DELETE request to the specified URL.

    Args:
        session (aiohttp.ClientSession): The aiohttp client session.
        url (str): The URL to send the DELETE request to.
        dataverse_url (str): The base URL of the Dataverse instance.

    Raises:
        aiohttp.ClientResponseError: If the DELETE request fails.
    """
    async with session.delete(urljoin(dataverse_url, url)) as response:
        response.raise_for_status()


async def _add_file_to_ds(
    session: aiohttp.ClientSession,
    dataverse_url: str,
    pid: str,
    file: File,
) -> bool:
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
    if not file.to_replace:
        url = urljoin(dataverse_url, UPLOAD_ENDPOINT + pid)
    else:
        url = build_url(
            dataverse_url=dataverse_url,
            endpoint=urljoin(
                dataverse_url,
                REPLACE_ENDPOINT.format(FILE_ID=file.file_id),
            ),
        )

    json_data = file.model_dump_json(
        by_alias=True,
        exclude={"to_replace", "file_id"},
        indent=2,
    )

    with aiohttp.MultipartWriter("form-data") as writer:
        json_part = writer.append(json_data)
        json_part.set_content_disposition("form-data", name="jsonData")

        async with session.post(url, data=writer) as response:
            return response.status == 200
