import json
import os
import grequests
from dvuploader.directupload import _setup_pbar
from dvuploader.file import File
from dvuploader.utils import build_url, retrieve_dataset_files
from tqdm.utils import CallbackIOWrapper


NATIVE_UPLOAD_ENDPOINT = "/api/datasets/:persistentId/add"
NATIVE_REPLACE_ENDPOINT = "/api/files/{FILE_ID}/replace"


def native_upload(
    file: File,
    dataverse_url: str,
    api_token: str,
    persistent_id: str,
    position: int,
):
    """
    Uploads a file to a Dataverse repository using the native upload method.

    Args:
        file (File): The file to be uploaded.
        dataverse_url (str): The URL of the Dataverse repository.
        api_token (str): The API token for authentication.
        persistent_id (str): The persistent identifier of the dataset.
        position (int): The position of the file within the dataset.

    Returns:
        Response: The response object from the upload request.
    """

    pbar = _setup_pbar(file.filepath, position)

    if not file.to_replace:
        url = build_url(
            dataverse_url=dataverse_url,
            endpoint=NATIVE_UPLOAD_ENDPOINT,
            persistentId=persistent_id,
        )
    else:
        url = build_url(
            dataverse_url=dataverse_url,
            endpoint=NATIVE_REPLACE_ENDPOINT.format(FILE_ID=file.file_id),
        )

    header = {"X-Dataverse-key": api_token}
    json_data = {
        "description": file.description,
        "forceReplace": "true",
        "directoryLabel": file.directoryLabel,
        "categories": file.categories,
        "restrict": file.restrict,
        "forceReplace": True,
    }

    files = {
        "jsonData": json.dumps(json_data),
        "file": (
            os.path.basename(file.filepath),
            CallbackIOWrapper(pbar.update, open(file.filepath, "rb"), "read"),
        ),
    }

    def _response_hook(response, *args, **kwargs):
        filesize = os.path.getsize(file.filepath)
        pbar.reset(filesize / 1024)
        pbar.update(filesize / 1024)
        pbar.close()
        return response

    return grequests.post(
        url=url,
        headers=header,
        files=files,
        hooks=dict(response=_response_hook),
    )
