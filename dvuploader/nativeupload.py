import asyncio
from io import BytesIO
import httpx
import json
import os
import tempfile
import tenacity
from typing import List, Tuple, Dict

from rich.progress import Progress, TaskID

from dvuploader.file import File
from dvuploader.packaging import distribute_files, zip_files
from dvuploader.utils import build_url, retrieve_dataset_files

MAX_RETRIES = int(os.environ.get("DVUPLOADER_MAX_RETRIES", 15))
NATIVE_UPLOAD_ENDPOINT = "/api/datasets/:persistentId/add"
NATIVE_REPLACE_ENDPOINT = "/api/files/{FILE_ID}/replace"
NATIVE_METADATA_ENDPOINT = "/api/files/{FILE_ID}/metadata"

assert isinstance(MAX_RETRIES, int), "DVUPLOADER_MAX_RETRIES must be an integer"


async def native_upload(
    files: List[File],
    dataverse_url: str,
    api_token: str,
    persistent_id: str,
    n_parallel_uploads: int,
    pbars,
    progress,
):
    """
    Executes native uploads for the given files in parallel.

    Args:
        files (List[File]): The list of File objects to be uploaded.
        dataverse_url (str): The URL of the Dataverse repository.
        api_token (str): The API token for the Dataverse repository.
        persistent_id (str): The persistent identifier of the Dataverse dataset.
        n_parallel_uploads (int): The number of parallel uploads to execute.

    Returns:
        List[requests.Response]: The list of responses for each file upload.
    """

    _reset_progress(pbars, progress)

    session_params = {
        "base_url": dataverse_url,
        "headers": {"X-Dataverse-key": api_token},
        "timeout": None,
        "limits": httpx.Limits(max_connections=n_parallel_uploads),
    }

    async with httpx.AsyncClient(**session_params) as session:
        with tempfile.TemporaryDirectory() as tmp_dir:
            packages = distribute_files(files)
            packaged_files = _zip_packages(
                packages=packages,
                tmp_dir=tmp_dir,
                progress=progress,
            )

            tasks = [
                _single_native_upload(
                    session=session,
                    file=file,
                    persistent_id=persistent_id,
                    pbar=pbar,  # type: ignore
                    progress=progress,
                )
                for pbar, file in packaged_files
            ]

            responses = await asyncio.gather(*tasks)
            _validate_upload_responses(responses, files)

            await _update_metadata(
                session=session,
                files=files,
                persistent_id=persistent_id,
                dataverse_url=dataverse_url,
                api_token=api_token,
            )


def _validate_upload_responses(
    responses: List[Tuple],
    files: List[File],
) -> None:
    """Validates the responses of the native upload requests."""

    for (status, response), file in zip(responses, files):
        if status == 200:
            continue

        print(f"❌ Failed to upload file '{file.file_name}': {response['message']}")


def _zip_packages(
    packages: List[Tuple[int, List[File]]],
    tmp_dir: str,
    progress: Progress,
) -> List[Tuple[TaskID, File]]:
    """
    Zips the given packages into zip files.

    Args:
        packages (List[Tuple[int, List[File]]]): The packages to be zipped.
        tmp_dir (str): The temporary directory to store the zip files in.

    Returns:
        List[File, TaskID]: The list of zip files.
    """

    files = []

    for index, package in packages:
        if len(package) == 1:
            file = package[0]
        else:
            file = File(
                filepath=zip_files(
                    files=package,
                    tmp_dir=tmp_dir,
                    index=index,
                ),
            )

            file.extract_file_name_hash_file()
            file.mimeType = "application/zip"

        pbar = progress.add_task(
            file.file_name,  # type: ignore
            total=file._size,
        )

        files.append((pbar, file))

    return files


def _reset_progress(
    pbars: List[TaskID],
    progress: Progress,
):
    """
    Resets the progress bars to zero.

    Args:
        pbars: The progress bars to reset.

    Returns:
        None
    """

    for pbar in pbars:
        progress.remove_task(pbar)


@tenacity.retry(
    wait=tenacity.wait_fixed(0.5),
    stop=tenacity.stop_after_attempt(MAX_RETRIES),
)
async def _single_native_upload(
    session: httpx.AsyncClient,
    file: File,
    persistent_id: str,
    pbar,
    progress,
):
    """
    Uploads a file to a Dataverse repository using the native upload method.

    Args:
        session (httpx.AsyncClient): The aiohttp client session.
        file (File): The file to be uploaded.
        persistent_id (str): The persistent identifier of the dataset.
        pbar: The progress bar object.
        progress: The progress object.

    Returns:
        tuple: A tuple containing the status code and the JSON response from the upload request.
    """

    if not file.to_replace:
        endpoint = build_url(
            endpoint=NATIVE_UPLOAD_ENDPOINT,
            persistentId=persistent_id,
        )
    else:
        endpoint = build_url(
            endpoint=NATIVE_REPLACE_ENDPOINT.format(FILE_ID=file.file_id),
        )

    json_data = _get_json_data(file)

    files = {
        "file": (file.file_name, file.handler, file.mimeType),
        "jsonData": (
            None,
            BytesIO(json.dumps(json_data).encode()),
            "application/json",
        ),
    }

    response = await session.post(
        endpoint,
        files=files,  # type: ignore
    )

    response.raise_for_status()

    if response.status_code == 200:
        progress.update(pbar, advance=file._size, complete=file._size)

        # Wait to avoid rate limiting
        await asyncio.sleep(0.7)

        return 200, response.json()

    # Wait to avoid rate limiting
    await asyncio.sleep(1.0)

    return False, {"message": "Failed to upload file"}


def _get_json_data(file: File) -> Dict:
    """Returns the JSON data for the native upload request."""
    return {
        "description": file.description,
        "directoryLabel": file.directory_label,
        "categories": file.categories,
        "restrict": file.restrict,
        "forceReplace": True,
    }


async def _update_metadata(
    session: httpx.AsyncClient,
    files: List[File],
    dataverse_url: str,
    api_token: str,
    persistent_id: str,
):
    """Updates the metadata of the given files in a Dataverse repository.

    Args:

        session (httpx.AsyncClient): The httpx async client.
        files (List[File]): The files to update the metadata for.
        dataverse_url (str): The URL of the Dataverse repository.
        api_token (str): The API token of the Dataverse repository.
        persistent_id (str): The persistent identifier of the dataset.
    """

    file_mapping = _retrieve_file_ids(
        persistent_id=persistent_id,
        dataverse_url=dataverse_url,
        api_token=api_token,
    )

    tasks = []

    for file in files:
        dv_path = os.path.join(file.directory_label, file.file_name)  # type: ignore

        try:
            file_id = file_mapping[dv_path]
        except KeyError:
            raise ValueError(
                (
                    f"File {dv_path} not found in Dataverse repository.",
                    "This may be due to the file not being uploaded to the repository.",
                )
            )

        task = _update_single_metadata(
            session=session,
            url=NATIVE_METADATA_ENDPOINT.format(FILE_ID=file_id),
            file=file,
        )

        tasks.append(task)

    await asyncio.gather(*tasks)


@tenacity.retry(
    wait=tenacity.wait_fixed(0.3),
    stop=tenacity.stop_after_attempt(MAX_RETRIES),
)
async def _update_single_metadata(
    session: httpx.AsyncClient,
    url: str,
    file: File,
) -> None:
    """Updates the metadata of a single file in a Dataverse repository."""

    json_data = _get_json_data(file)

    del json_data["forceReplace"]
    del json_data["restrict"]

    # Send metadata as a readable byte stream
    # This is a workaround since "data" and "json"
    # does not work
    files = {
        "jsonData": (
            None,
            BytesIO(json.dumps(json_data).encode()),
            "application/json",
        ),
    }

    response = await session.post(url, files=files)

    if response.status_code == 200:
        return
    else:
        await asyncio.sleep(1.0)

    raise ValueError(f"Failed to update metadata for file {file.file_name}.")


def _retrieve_file_ids(
    persistent_id: str,
    dataverse_url: str,
    api_token: str,
) -> Dict[str, str]:
    """Retrieves the file IDs of the given files.

    Args:
        files (List[File]): The files to retrieve the IDs for.
        persistent_id (str): The persistent identifier of the dataset.
        dataverse_url (str): The URL of the Dataverse repository.
        api_token (str): The API token of the Dataverse repository.

    Returns:
        Dict[str, str]: The list of file IDs.
    """

    # Fetch file metadata
    ds_files = retrieve_dataset_files(
        persistent_id=persistent_id,
        dataverse_url=dataverse_url,
        api_token=api_token,
    )

    return _create_file_id_path_mapping(ds_files)


def _create_file_id_path_mapping(files):
    """Creates dictionary that maps from directoryLabel + filename to ID"""
    mapping = {}

    for file in files:
        directory_label = file.get("directoryLabel", "")
        file = file["dataFile"]
        path = os.path.join(directory_label, file["filename"])
        mapping[path] = file["id"]

    return mapping
