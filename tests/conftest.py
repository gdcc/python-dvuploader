import os
import random
from typing import Literal, Tuple, Union, overload

import httpx
import pytest


@pytest.fixture
def credentials():
    """Returns the credentials for the Dataverse server."""
    BASE_URL = os.environ.get("BASE_URL")
    API_TOKEN = os.environ.get("API_TOKEN")

    assert BASE_URL, "BASE_URL environment variable must be set"
    assert API_TOKEN, "API_TOKEN environment variable must be set"

    return BASE_URL, API_TOKEN


@overload
def create_dataset(
    parent: str,
    server_url: str,
    api_token: str,
    return_id: Literal[False] = False,
) -> str: ...


@overload
def create_dataset(
    parent: str,
    server_url: str,
    api_token: str,
    return_id: Literal[True],
) -> Tuple[str, int]: ...


def create_dataset(
    parent: str,
    server_url: str,
    api_token: str,
    return_id: bool = False,
) -> Union[str, Tuple[str, int]]:
    """
    Creates a dataset in a Dataverse.

    Args:
        parent (str): The parent Dataverse identifier.
        server_url (str): The URL of the Dataverse server.
        api_token (str): The API token for authentication.

    Returns:
        Dict: The response from the Dataverse API.
    """
    if server_url.endswith("/"):
        server_url = server_url[:-1]

    url = f"{server_url}/api/dataverses/{parent}/datasets"
    response = httpx.post(
        url=url,
        headers={"X-Dataverse-key": api_token},
        data=open("./tests/fixtures/create_dataset.json", "rb"),  # type: ignore[reportUnboundVariable]
    )

    response.raise_for_status()

    if return_id:
        return response.json()["data"]["persistentId"], response.json()["data"]["id"]
    else:
        return response.json()["data"]["persistentId"]


def create_mock_file(
    directory: str,
    name: str,
    size: int = 100,
):
    """Create a file with the specified size in megabytes.

    Args:
        directory (str): The directory where the file will be created.
        name (str): The name of the file.
        size (int, optional): Size of the file in megabytes. Defaults to 100.
    """

    path = os.path.join(directory, name)
    size = size * 1024 * 1024

    with open(path, "wb") as f:
        f.seek(size - 1)  # 1 GB
        f.write(b"\0")

    return path


def create_mock_tabular_file(
    directory: str,
    name: str,
    rows: int = 1000000,
    cols: int = 10,
):
    """Create a tabular file with the specified number of rows and columns.

    Args:
        directory (str): The directory where the file will be created.
        name (str): The name of the file.
        rows (int, optional): The number of rows in the file. Defaults to 1000000.
        cols (int, optional): The number of columns in the file. Defaults to 10.
    """
    path = os.path.join(directory, name)
    with open(path, "w") as f:
        # Create header
        f.write(",".join([f"col_{i}" for i in range(cols)]) + "\n")

        # Create rows
        for i in range(rows):
            f.write(
                f"{i}"
                + ","
                + ",".join([f"{random.randint(0, 100)}" for j in range(cols - 1)])
                + "\n"
            )

    return path
