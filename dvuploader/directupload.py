import json
import os
from typing import Dict, List
from urllib.parse import urljoin

import grequests
import requests
from dotted_dict import DottedDict
from requests.exceptions import HTTPError
from requests.models import PreparedRequest
from tqdm import tqdm
from tqdm.utils import CallbackIOWrapper

from dvuploader.file import File
from dvuploader.chunkstream import ChunkStream

global MAX_RETRIES

MAX_RETRIES = 10
TICKET_ENDPOINT = "/api/datasets/:persistentId/uploadurls"
ADD_FILE_ENDPOINT = "/api/datasets/:persistentId/addFiles"
UPLOAD_ENDPOINT = "/api/datasets/:persistentId/add?persistentId="


def direct_upload(
    file: File,
    persistent_id: str,
    dataverse_url: str,
    api_token: str,
    position: int,
) -> bool:
    """
    Uploads a file to a Dataverse collection using direct upload.

    Args:
        file (File): The file object to upload.
        persistent_id (str): The persistent identifier of the Dataverse dataset to upload to.
        dataverse_url (str): The URL of the Dataverse instance to upload to.
        api_token (str): The API token to use for authentication.
        position (int): The position of the file in the list of files to upload.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """

    assert file.fileName is not None, "File name is None"
    assert os.path.exists(file.filepath), f"File {file.filepath} does not exist"

    file_size = os.path.getsize(file.filepath)
    pbar = _setup_pbar(file.filepath, position)
    response = _request_ticket(
        dataverse_url=dataverse_url,
        api_token=api_token,
        file_size=file_size,
        persistent_id=persistent_id,
    )

    if not "urls" in response:
        file.storageIdentifier = _upload_singlepart(
            response=response,
            filepath=file.filepath,
            pbar=pbar,
        )
    else:
        file.storageIdentifier = _upload_multipart(
            response=response,
            filepath=file.filepath,
            dataverse_url=dataverse_url,
            api_token=api_token,
            pbar=pbar,
        )

    result = _add_file_to_ds(
        dataverse_url,
        persistent_id,
        api_token,
        file,
    )

    if result is True:
        pbar.bar_format = f"├── {file.filepath} uploaded!"
    else:
        pbar.bar_format = f"├── {file.filepath} failed to upload!"

    pbar.close()

    return True


def _request_ticket(
    dataverse_url: str,
    api_token: str,
    persistent_id: str,
    file_size: int,
) -> Dict:
    """Requests a ticket from a Dataverse collection to perform an upload.

    This method will return a URL and storageIdentifier that later on is
    used to perform the direct upload.

    Please note, your Dataverse installation and collection should have
    enabled Direct Upload to an S3 bucket to perform the upload.

    Args:
        dataverse_url (str): URL to the Dataverse installation
        api_token (str): API Token used to access the dataset.
        persistent_id (str): Persistent identifier of the dataset of interest.

    Returns:
        Dict: Response from the Dataverse API
    """

    # Build request URL
    query = _build_url(
        endpoint=TICKET_ENDPOINT,
        dataverse_url=dataverse_url,
        key=api_token,
        persistentId=persistent_id,
        size=file_size,
    )

    # Send HTTP request
    response = requests.get(query)

    if response.status_code != 200:
        raise HTTPError(
            f"Could not request a ticket for dataset '{persistent_id}' at '{dataverse_url}' \
                \n\n{json.dumps(response.json(), indent=2)}"
        )

    return DottedDict(response.json()["data"])


def _build_url(
    dataverse_url: str,
    endpoint: str,
    **kwargs,
) -> str:
    """Builds a URL string, given access points and credentials"""

    req = PreparedRequest()
    req.prepare_url(urljoin(dataverse_url, endpoint), kwargs)

    assert req.url is not None, f"Could not build URL for '{dataverse_url}'"

    return req.url


def _upload_singlepart(
    response: Dict,
    filepath: str,
    pbar: tqdm,
) -> str:
    """
    Uploads a single part of a file to a remote server using HTTP PUT method.

    Args:
        response (Dict): A dictionary containing the response from the server.
        filepath (str): The path to the file to be uploaded.
        pbar (tqdm): A progress bar object to track the upload progress.

    Returns:
        str: The storage identifier of the uploaded file.
    """

    assert "url" in response, "Couldnt find 'url'"

    headers = {"x-amz-tagging": "dv-state=temp"}
    storage_identifier = response.storageIdentifier  # type: ignore
    wrapped_file = CallbackIOWrapper(pbar.update, open(filepath, "rb"))
    resp = requests.put(
        response.url,  # type: ignore
        data=wrapped_file,  # type: ignore
        stream=True,
        headers=headers,
    )

    if resp.status_code != 200:
        raise HTTPError(
            f"Could not upload file \
                \n\n{resp.headers}"
        )

    return storage_identifier


def _upload_multipart(
    response: Dict,
    filepath: str,
    dataverse_url: str,
    api_token: str,
    pbar: tqdm,
):
    """
    Uploads a file to Dataverse using multipart upload.

    Args:
        response (Dict): The response from the Dataverse API containing the upload ticket information.
        filepath (str): The path to the file to be uploaded.
        dataverse_url (str): The URL of the Dataverse instance.
        api_token (str): The API token for the Dataverse instance.
        pbar (tqdm): A progress bar to track the upload progress.

    Returns:
        str: The storage identifier for the uploaded file.
    """

    _validate_ticket_response(response)

    abort = response.abort  # type: ignore
    complete = response.complete  # type: ignore
    part_size = response.partSize  # type: ignore
    urls = response.urls  # type: ignore
    storage_identifier = response.storageIdentifier  # type: ignore

    # Chunk file and retrieve paths and urls
    chunks = _chunk_file(filepath, part_size, urls)
    tasks = [{"file": streamer, "url": url} for streamer, url in chunks]

    try:
        rs = (
            grequests.put(
                task["url"],
                data=CallbackIOWrapper(pbar.update, task["file"], "read"),
                stream=True,
            )
            for task in tasks
        )

        # Execute upload
        responses = grequests.map(rs)
        e_tags = [response.headers["ETag"] for response in responses]

    except Exception as e:
        _abort_upload(abort, dataverse_url, api_token)
        raise e

    _complete_upload(complete, dataverse_url, e_tags, api_token)

    return storage_identifier


def _validate_ticket_response(response: Dict) -> None:
    """Validate the response from the ticket request to include all necessary fields."""

    assert "abort" in response, "Couldnt find 'abort'"
    assert "complete" in response, "Couldnt find 'complete'"
    assert "partSize" in response, "Couldnt find 'partSize'"
    assert "urls" in response, "Couldnt find 'urls'"
    assert "storageIdentifier" in response, "Could not find 'storageIdentifier'"


def _chunk_file(
    path: str,
    chunk_size: int,
    urls: Dict,
) -> List[str]:
    """
    Breaks a file into chunks of a specified size and saves them to disk.
    Returns a list of tuples containing the path to each chunk and its corresponding upload URL.

    Args:
        path (str): The path to the file to be chunked.
        chunk_size (int): The size of each chunk in bytes.
        urls (Dict): A dictionary containing the upload URLs for each chunk.
        chunk_dir (str, optional): The directory to save the chunks in. Defaults to "./chunks".

    Returns:
        List[str]: A list of tuples containing the path to each chunk and its corresponding upload URL.
    """

    # os.makedirs(chunk_dir, exist_ok=True)

    start = 0
    uploads = []

    for url in urls.values():
        size = min(chunk_size, os.stat(path).st_size - start)
        file = open(path, "rb")
        file.seek(start)

        uploads.append((ChunkStream(file, chunk_size, size), url))

        start += chunk_size

    return uploads


def _complete_upload(
    url: str,
    dataverse_url: str,
    e_tags: List[str],
    api_token: str,
) -> None:
    """Completes the upload by sending the E tags"""

    headers = {"X-Dataverse-key": api_token}
    payload = json.dumps({str(index + 1): e_tag for index, e_tag in enumerate(e_tags)})
    response = requests.put(urljoin(dataverse_url, url), data=payload, headers=headers)

    if response.status_code != 200:
        raise HTTPError(
            f"Could not complete upload \
                \n\n{json.dumps(response.json(), indent=2)}"
        )


def _abort_upload(
    url: str,
    dataverse_url: str,
    api_token: str,
):
    headers = {"X-Dataverse-key": api_token}
    requests.delete(urljoin(dataverse_url, url), headers=headers)


def _add_file_to_ds(
    dataverse_url: str,
    pid: str,
    api_token: str,
    file: File,
):
    headers = {"X-Dataverse-key": api_token}
    url = urljoin(dataverse_url, UPLOAD_ENDPOINT + pid)
    payload = {"jsonData": file.json(by_alias=True)}

    for _ in range(MAX_RETRIES):
        response = requests.post(url, headers=headers, files=payload)
        if response.status_code == 200:
            return True

    return False


def _setup_pbar(fpath: str, position: int, pre: str = "├── "):
    return tqdm(
        total=os.stat(fpath).st_size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        ascii=" >=",
        desc=pre + f"{fpath} ",
        bar_format="{l_bar}{bar:20}{r_bar}{bar:-10b}",
        position=position,
        leave=True,
    )
