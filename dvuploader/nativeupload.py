import asyncio
import json
import os
from typing import List
import aiohttp

from dvuploader.file import File
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

    session_params = {
        "base_url": dataverse_url,
        "headers": {"X-Dataverse-key": api_token},
        "connector": aiohttp.TCPConnector(
            limit=n_parallel_uploads,
            force_close=True,
        ),
    }

    async with aiohttp.ClientSession(**session_params) as session:
        tasks = [
            _single_native_upload(
                session=session,
                file=file,
                persistent_id=persistent_id,
                pbar=pbar,  # type: ignore
                progress=progress,
            )
            for pbar, file in zip(pbars, files)
        ]

        responses = await asyncio.gather(*tasks)

    for (status, response), file in zip(responses, files):
        if status == 200:
            continue

        print(f"❌ Failed to upload file '{file.fileName}': {response['message']}")


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
        "forceReplace": "true",
        "directoryLabel": file.directoryLabel,
        "categories": file.categories,
        "restrict": file.restrict,
        "forceReplace": True,
    }

    for _ in range(MAX_RETRIES):
        with aiohttp.MultipartWriter("form-data") as writer:
            json_part = writer.append(json.dumps(json_data))
            json_part.set_content_disposition("form-data", name="jsonData")

            file_part = writer.append(open(file.filepath, "rb"))
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
