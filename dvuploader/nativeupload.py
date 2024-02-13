import asyncio
import json
import os
import tempfile
from typing import List, Tuple
import aiofiles
import aiohttp

from rich.progress import Progress, TaskID

from dvuploader.file import File
from dvuploader.packaging import distribute_files, zip_files
from dvuploader.utils import build_url

MAX_RETRIES = os.environ.get("DVUPLOADER_MAX_RETRIES", 15)
NATIVE_UPLOAD_ENDPOINT = "/api/datasets/:persistentId/add"
NATIVE_REPLACE_ENDPOINT = "/api/files/{FILE_ID}/replace"

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
        "connector": aiohttp.TCPConnector(
            limit=n_parallel_uploads,
            timeout_ceil_threshold=120,
        ),
    }

    async with aiohttp.ClientSession(**session_params) as session:
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

    for (status, response), file in zip(responses, files):
        if status == 200:
            continue

        print(f"❌ Failed to upload file '{file.fileName}': {response['message']}")


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

            file.extract_filename_hash_file()
            file.mimeType = "application/zip"

        pbar = progress.add_task(
            file.fileName,  # type: ignore
            total=os.path.getsize(file.filepath),
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


async def _single_native_upload(
    session: aiohttp.ClientSession,
    file: File,
    persistent_id: str,
    pbar,
    progress,
):
    """
    Uploads a file to a Dataverse repository using the native upload method.

    Args:
        session (aiohttp.ClientSession): The aiohttp client session.
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

    json_data = {
        "description": file.description,
        "forceReplace": True,
        "directoryLabel": file.directoryLabel,
        "categories": file.categories,
        "restrict": file.restrict,
        "forceReplace": True,
    }

    for _ in range(MAX_RETRIES):
        with aiohttp.MultipartWriter("form-data") as writer:
            json_part = writer.append(json.dumps(json_data))
            json_part.set_content_disposition("form-data", name="jsonData")

            file_part = writer.append(
                file_sender(
                    file_name=file.filepath,
                    progress=progress,
                    pbar=pbar,
                )
            )
            file_part.set_content_disposition(
                "form-data",
                name="file",
                filename=file.fileName,
            )
            async with session.post(endpoint, data=writer) as response:
                status = response.status

                if status == 200:
                    progress.update(
                        pbar,
                        advance=os.path.getsize(file.filepath),
                    )

                    # Wait to avoid rate limiting
                    await asyncio.sleep(0.7)

                    return status, await response.json()

        # Wait to avoid rate limiting
        await asyncio.sleep(1.0)

    return False, {"message": "Failed to upload file"}


async def file_sender(
    file_name: str,
    progress: Progress,
    pbar: TaskID,
):
    """
    Asynchronously reads and yields chunks of a file.

    Args:
        file_name (str): The name of the file to read.
        progress (Progress): The progress object to track the file upload progress.
        pbar (TaskID): The ID of the progress bar associated with the file upload.

    Yields:
        bytes: The chunks of the file.

    """
    chunk_size = 64 * 1024  # 10 MB
    async with aiofiles.open(file_name, "rb") as f:
        chunk = await f.read(chunk_size)
        progress.advance(pbar, advance=chunk_size)
        while chunk:
            yield chunk
            chunk = await f.read(chunk_size)
            progress.advance(pbar, advance=chunk_size)
