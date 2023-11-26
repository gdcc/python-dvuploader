from urllib.parse import urljoin
from requests import PreparedRequest


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
