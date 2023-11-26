import json
from urllib.parse import urljoin
from requests import PreparedRequest
import requests
from dotted_dict import DottedDict


def build_url(
    dataverse_url: str,
    endpoint: str,
    **kwargs,
) -> str:
    """Builds a URL string, given access points and credentials"""

    req = PreparedRequest()
    req.prepare_url(urljoin(dataverse_url, endpoint), kwargs)

    assert req.url is not None, f"Could not build URL for '{dataverse_url}'"

    return req.url


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

    DATASET_ENDPOINT = "/api/datasets/:persistentId/?persistentId={0}"

    response = requests.get(
        urljoin(dataverse_url, DATASET_ENDPOINT.format(persistent_id)),
        headers={"X-Dataverse-key": api_token},
    )

    if response.status_code != 200:
        raise requests.HTTPError(
            f"Could not download dataset '{persistent_id}' at '{dataverse_url}' \
                    \n\n{json.dumps(response.json(), indent=2)}"
        )  # type: ignore

    return DottedDict(response.json()).data.latestVersion.files
