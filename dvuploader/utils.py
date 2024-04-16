import json
import os
import pathlib
import re
from typing import List
from urllib.parse import urljoin
import requests
from rich.progress import Progress

from dvuploader.file import File
import os


def build_url(
    endpoint: str,
    **kwargs,
) -> str:
    """Builds a URL string, given access points and credentials

    Args:
        endpoint (str): The base endpoint of the URL
        **kwargs: Additional key-value pairs to be included as query parameters

    Returns:
        str: The complete URL string with query parameters
    """

    if not isinstance(endpoint, str):
        raise TypeError("Endpoint must be a string")

    assert all(isinstance(v, str) for v in kwargs.keys()), "All keys must be strings"

    queries = "&".join([f"{k}={str(v)}" for k, v in kwargs.items()])

    if kwargs == {}:
        return endpoint

    return f"{endpoint}?{queries}"


def retrieve_dataset_files(
    dataverse_url: str,
    persistent_id: str,
    api_token: str,
):
    """
    Retrieve the files of a specific dataset from a Dataverse repository.

    Parameters:
        dataverse_url (str): The base URL of the Dataverse repository.
        persistent_id (str): The persistent identifier (PID) of the dataset.

    Returns:
        list: A list of files in the dataset.

    Raises:
        HTTPError: If the request to the Dataverse repository fails.
    """

    DATASET_ENDPOINT = f"/api/datasets/:persistentId/?persistentId={persistent_id}"

    response = requests.get(
        urljoin(dataverse_url, DATASET_ENDPOINT),
        headers={"X-Dataverse-key": api_token},
    )

    response.raise_for_status()

    return response.json()["data"]["latestVersion"]["files"]


def add_directory(
    directory: str,
    ignore: List[str] = [r"^\."],
    rootDirectoryLabel: str = "",
):
    """
    Recursively adds all files in the specified directory to a list of File objects.

    Args:
        directory (str): The directory path.
        ignore (List[str], optional): A list of regular expressions to ignore certain files or directories. Defaults to [r"^\."].
        rootDirectoryLabel (str, optional): The label to be added to the directory path of each file. Defaults to "".

    Returns:
        List[File]: A list of File objects representing the files in the directory.
    """
    files = []

    # Part of the path to remove from the directory
    for file in pathlib.Path(directory).rglob("*"):
        if not file.is_file():
            continue
        if part_is_ignored(str(file.name), ignore):
            continue
        if any(part_is_ignored(part, ignore) for part in list(file.parts)):
            continue

        directory_label = _truncate_path(
            file.parent,
            pathlib.Path(directory),
        )

        files.append(
            File(
                filepath=str(file),
                directoryLabel=os.path.join(
                    rootDirectoryLabel,
                    directory_label,
                ),
            )
        )

    return files


def _truncate_path(path: pathlib.Path, to_remove: pathlib.Path):
    """
    Truncate a path by removing a substring from the beginning.

    Args:
        path (str): The path to truncate.
        to_remove (str): The substring to remove from the beginning of the path.

    Returns:
        str: The truncated path.
    """

    parts = path.parts[len(to_remove.parts) :]

    if len(parts) == 0:
        return ""

    return os.path.join(*[str(part) for part in parts])


def part_is_ignored(part, ignore):
    """
    Check if a part should be ignored based on a list of patterns.

    Args:
        part (str): The part to check.
        ignore (list): A list of patterns to match against.

    Returns:
        bool: True if the part should be ignored, False otherwise.
    """
    for pattern in ignore:
        if re.match(pattern, part):
            return True
    return False


def setup_pbar(
    file: File,
    progress: Progress,
) -> int:
    """
    Set up a progress bar for a file.

    Args:
        fpath (str): The path to the file.
        progress (Progress): The progress bar object.

    Returns:
        int: The task ID of the progress bar.
    """

    file_size = file._size
    fname = file.file_name

    return progress.add_task(
        f"[pink]├── {fname}",
        start=True,
        total=file_size,
    )
