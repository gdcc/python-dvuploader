import asyncio
from io import BytesIO
from pathlib import Path
import httpx
import json
import os
import tempfile
import rich
import tenacity
from typing import List, Optional, Tuple, Dict

from rich.progress import Progress, TaskID

from dvuploader.file import File
from dvuploader.packaging import distribute_files, zip_files
from dvuploader.utils import build_url, retrieve_dataset_files

##### CONFIGURATION #####

# Based on MAX_RETRIES, we will wait between 0.3 and 120 seconds between retries:
# Exponential recursion: 0.1 * 2^n
#
# This will exponentially increase the wait time between retries.
# The max wait time is 240 seconds per retry though.
MAX_RETRIES = int(os.environ.get("DVUPLOADER_MAX_RETRIES", 15))
MAX_RETRY_TIME = int(os.environ.get("DVUPLOADER_MAX_RETRY_TIME", 60))
MIN_RETRY_TIME = int(os.environ.get("DVUPLOADER_MIN_RETRY_TIME", 1))
RETRY_MULTIPLIER = float(os.environ.get("DVUPLOADER_RETRY_MULTIPLIER", 0.1))
RETRY_STRAT = tenacity.wait_exponential(
    multiplier=RETRY_MULTIPLIER,
    min=MIN_RETRY_TIME,
    max=MAX_RETRY_TIME,
)

assert isinstance(MAX_RETRIES, int), "DVUPLOADER_MAX_RETRIES must be an integer"
assert isinstance(MAX_RETRY_TIME, int), "DVUPLOADER_MAX_RETRY_TIME must be an integer"
assert isinstance(MIN_RETRY_TIME, int), "DVUPLOADER_MIN_RETRY_TIME must be an integer"
assert isinstance(RETRY_MULTIPLIER, float), (
    "DVUPLOADER_RETRY_MULTIPLIER must be a float"
)

##### END CONFIGURATION #####

NATIVE_UPLOAD_ENDPOINT = "/api/datasets/:persistentId/add"
NATIVE_REPLACE_ENDPOINT = "/api/files/{FILE_ID}/replace"
NATIVE_METADATA_ENDPOINT = "/api/files/{FILE_ID}/metadata"

TABULAR_EXTENSIONS = [
    "csv",
    "tsv",
]

##### ERROR MESSAGES #####

ZIP_LIMIT_MESSAGE = "The number of files in the zip archive is over the limit"


async def native_upload(
    files: List[File],
    dataverse_url: str,
    api_token: str,
    persistent_id: str,
    n_parallel_uploads: int,
    pbars,
    progress,
    proxy: Optional[str] = None,
):
    """
    Executes native uploads for the given files in parallel.

    Args:
        files (List[File]): The list of File objects to be uploaded.
        dataverse_url (str): The URL of the Dataverse repository.
        api_token (str): The API token for the Dataverse repository.
        persistent_id (str): The persistent identifier of the Dataverse dataset.
        n_parallel_uploads (int): The number of parallel uploads to execute.
        pbars: List of progress bar IDs to track upload progress.
        progress: Progress object to manage progress bars.
        proxy (str): The proxy to use for the upload.
    Returns:
        None
    """

    _reset_progress(pbars, progress)

    session_params = {
        "base_url": dataverse_url,
        "headers": {"X-Dataverse-key": api_token},
        "timeout": None,
        "limits": httpx.Limits(max_connections=n_parallel_uploads),
        "proxy": proxy,
    }

    files_new = [file for file in files if not file.to_replace]
    files_new_metadata = [
        file for file in files if file.to_replace and file._unchanged_data
    ]
    files_replace = [
        file for file in files if file.to_replace and not file._unchanged_data
    ]

    # These are not in a package but need a metadtata update, ensure even for zips
    for file in files_new_metadata:
        file._enforce_metadata_update = True

    async with httpx.AsyncClient(**session_params) as session:
        with tempfile.TemporaryDirectory() as tmp_dir:
            packages = distribute_files(files_new)
            packaged_files = _zip_packages(
                packages=packages,
                tmp_dir=tmp_dir,
                progress=progress,
            )

            replacable_files = [
                (
                    progress.add_task(
                        file.file_name,  # type: ignore
                        total=file._size,
                    ),
                    file,
                )
                for file in files_replace
            ]

            tasks = [
                _single_native_upload(
                    session=session,
                    file=file,
                    persistent_id=persistent_id,
                    pbar=pbar,  # type: ignore
                    progress=progress,
                )
                for pbar, file in (packaged_files + replacable_files)
            ]

            responses = await asyncio.gather(*tasks)
            _validate_upload_responses(responses, files)

            await _update_metadata(
                session=session,
                files=files_new + files_new_metadata,
                persistent_id=persistent_id,
                dataverse_url=dataverse_url,
                api_token=api_token,
            )


def _validate_upload_responses(
    responses: List[Tuple],
    files: List[File],
) -> None:
    """
    Validates the responses of the native upload requests.

    Args:
        responses (List[Tuple]): List of tuples containing status code and response data.
        files (List[File]): List of files that were uploaded.

    Returns:
        None
    """

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
        progress (Progress): Progress object to manage progress bars.

    Returns:
        List[Tuple[TaskID, File]]: List of tuples containing progress bar ID and File object.
    """

    files = []

    for index, package in packages:
        if len(package) == 1:
            file = package[0]
            pbar = progress.add_task(
                file.file_name,  # type: ignore
                total=file._size,
            )
        else:
            path = zip_files(
                files=package,
                tmp_dir=tmp_dir,
                index=index,
            )

            file = File(filepath=path)
            file.extract_file_name()
            file.mimeType = "application/zip"

            pbar = progress.add_task(
                f"Zip package of {len(package)} files",  # type: ignore
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
        pbars (List[TaskID]): List of progress bar IDs to reset.
        progress (Progress): Progress object managing the progress bars.

    Returns:
        None
    """

    for pbar in pbars:
        progress.remove_task(pbar)


@tenacity.retry(
    wait=RETRY_STRAT,
    stop=tenacity.stop_after_attempt(MAX_RETRIES),
    retry=tenacity.retry_if_exception_type((httpx.HTTPStatusError,)),
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
        session (httpx.AsyncClient): The httpx client session.
        file (File): The file to be uploaded.
        persistent_id (str): The persistent identifier of the dataset.
        pbar: Progress bar ID for tracking upload progress.
        progress: Progress object managing the progress bars.

    Returns:
        tuple: A tuple containing:
            - int: Status code (200 for success, False for failure)
            - dict: JSON response from the upload request
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
    handler = file.get_handler()

    files = {
        "file": (
            file.file_name,
            handler,
            file.mimeType,
        ),
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

    if response.status_code == 400 and response.json()["message"].startswith(
        ZIP_LIMIT_MESSAGE
    ):
        # Explicitly handle the zip limit error, because otherwise we will run into
        # unnecessary retries.
        raise ValueError(
            f"Could not upload file '{file.file_name}' due to zip limit:\n{response.json()['message']}"
        )

    # Any other error is re-raised and the error will be handled by the retry logic.
    response.raise_for_status()

    if response.status_code == 200:
        # If we did well, update the progress bar.
        progress.update(pbar, advance=file._size, complete=file._size)

        # Wait to avoid rate limiting
        await asyncio.sleep(0.7)

        return 200, response.json()

    # Wait to avoid rate limiting
    await asyncio.sleep(1.0)
    return False, {"message": "Failed to upload file"}


def _get_json_data(file: File) -> Dict:
    """
    Returns the JSON data for the native upload request.

    Args:
        file (File): The file to create JSON data for.

    Returns:
        Dict: Dictionary containing file metadata for the upload request.
    """

    metadata = {
        "description": file.description,
        "categories": file.categories,
        "restrict": file.restrict,
        "forceReplace": True,
    }

    if file.directory_label:
        metadata["directoryLabel"] = file.directory_label

    return metadata


async def _update_metadata(
    session: httpx.AsyncClient,
    files: List[File],
    dataverse_url: str,
    api_token: str,
    persistent_id: str,
):
    """
    Updates the metadata of the given files in a Dataverse repository.

    Args:
        session (httpx.AsyncClient): The httpx async client.
        files (List[File]): The files to update the metadata for.
        dataverse_url (str): The URL of the Dataverse repository.
        api_token (str): The API token of the Dataverse repository.
        persistent_id (str): The persistent identifier of the dataset.

    Raises:
        ValueError: If a file is not found in the Dataverse repository.
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
            if _tab_extension(dv_path) in file_mapping:
                file_id = file_mapping[_tab_extension(dv_path)]
            elif (
                file.file_name
                and _is_zip(file.file_name)
                and not file._is_inside_zip
                and not file._enforce_metadata_update
            ):
                # When the file is a zip package it will be unpacked and thus
                # the expected file name of the zip will not be in the
                # dataset, since it has been unpacked.
                continue
            else:
                file_id = file_mapping[dv_path]
        except KeyError:
            rich.print(
                (
                    f"File {dv_path} not found in Dataverse repository.",
                    "This may be due to the file not being uploaded to the repository:",
                )
            )
            continue

        task = _update_single_metadata(
            session=session,
            url=NATIVE_METADATA_ENDPOINT.format(FILE_ID=file_id),
            file=file,
        )

        tasks.append(task)

    await asyncio.gather(*tasks)


@tenacity.retry(
    wait=RETRY_STRAT,
    stop=tenacity.stop_after_attempt(MAX_RETRIES),
)
async def _update_single_metadata(
    session: httpx.AsyncClient,
    url: str,
    file: File,
) -> None:
    """
    Updates the metadata of a single file in a Dataverse repository.

    Args:
        session (httpx.AsyncClient): The httpx async client.
        url (str): The URL endpoint for updating metadata.
        file (File): The file to update metadata for.

    Raises:
        ValueError: If metadata update fails.
    """

    json_data = _get_json_data(file)

    del json_data["forceReplace"]

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
    """
    Retrieves the file IDs of files in a dataset.

    Args:
        persistent_id (str): The persistent identifier of the dataset.
        dataverse_url (str): The URL of the Dataverse repository.
        api_token (str): The API token of the Dataverse repository.

    Returns:
        Dict[str, str]: Dictionary mapping file paths to their IDs.
    """

    # Fetch file metadata
    ds_files = retrieve_dataset_files(
        persistent_id=persistent_id,
        dataverse_url=dataverse_url,
        api_token=api_token,
    )

    return _create_file_id_path_mapping(ds_files)


def _create_file_id_path_mapping(files):
    """
    Creates dictionary that maps from directoryLabel + filename to ID.

    Args:
        files: List of file metadata from Dataverse.

    Returns:
        Dict[str, str]: Dictionary mapping file paths to their IDs.
    """
    mapping = {}

    for file in files:
        directory_label = file.get("directoryLabel", "")
        file = file["dataFile"]
        path = os.path.join(directory_label, file["filename"])
        mapping[path] = file["id"]

    return mapping


def _tab_extension(path: str) -> str:
    """
    Adds a tabular extension to the path if it is not already present.
    """
    return str(Path(path).with_suffix(".tab"))


def _is_zip(file_name: str) -> bool:
    """
    Checks if a file name ends with a zip extension.
    """
    return file_name.endswith(".zip")
