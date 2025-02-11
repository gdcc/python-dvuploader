import os
import pathlib
import re
from typing import List
from urllib.parse import urljoin
import httpx
from rich.progress import Progress

from dvuploader.file import File


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

    Args:
        dataverse_url (str): The base URL of the Dataverse repository.
        persistent_id (str): The persistent identifier (PID) of the dataset.
        api_token (str): API token for authentication.

    Returns:
        list: A list of files in the dataset.

    Raises:
        HTTPError: If the request to the Dataverse repository fails.
    """

    DATASET_ENDPOINT = f"/api/datasets/:persistentId/?persistentId={persistent_id}"

    response = httpx.get(
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
        directory (str): The directory path to scan for files.
        ignore (List[str], optional): A list of regular expressions to ignore certain files or directories. Defaults to [r"^\."].
        rootDirectoryLabel (str, optional): The label to be prepended to the directory path of each file. Defaults to "".

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
    Truncate a path by removing a prefix path.

    Args:
        path (pathlib.Path): The full path to truncate.
        to_remove (pathlib.Path): The prefix path to remove.

    Returns:
        str: The truncated path as a string, or empty string if nothing remains after truncation.
    """

    parts = path.parts[len(to_remove.parts) :]

    if len(parts) == 0:
        return ""

    return os.path.join(*[str(part) for part in parts])


def part_is_ignored(part, ignore):
    """
    Check if a path part should be ignored based on a list of regex patterns.

    Args:
        part (str): The path part to check.
        ignore (List[str]): A list of regex patterns to match against.

    Returns:
        bool: True if the part matches any ignore pattern, False otherwise.
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
    Set up a progress bar for tracking file upload progress.

    Args:
        file (File): The File object containing file information.
        progress (Progress): The rich Progress instance for displaying progress.

    Returns:
        int: The task ID for the created progress bar.
    """

    file_size = file._size
    fname = file.file_name

    return progress.add_task(
        f"[pink]├── {fname}",
        start=True,
        total=file_size,
    )
