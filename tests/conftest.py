import os
import random
import signal
import socket
import subprocess
import sys
import time

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


def create_dataset(
    parent: str,
    server_url: str,
    api_token: str,
):
    """
    Creates a dataset in a Dataverse.

    Args:
        parent (str): The parent Dataverse identifier.
        server_url (str): The URL of the Dataverse server.
        api_token (str): The API token for authentication.

    Returns:
        str: The persistent identifier of the created dataset.
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


def _wait_for_port(host: str, port: int, timeout: float = 5.0) -> None:
    """Wait until a TCP port is open on host within timeout seconds."""
    start = time.time()
    while time.time() - start < timeout:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            try:
                if sock.connect_ex((host, port)) == 0:
                    return
            except OSError:
                pass
        time.sleep(0.1)
    raise TimeoutError(f"Proxy did not start on {host}:{port} within {timeout}s")


@pytest.fixture(scope="function")
def http_proxy_server():
    """Start a local HTTP proxy on 127.0.0.1:3128 for tests that require it."""
    host = "127.0.0.1"
    port = 3128

    # Ensure dependency is available
    try:
        import proxy  # noqa: F401
    except Exception as exc:  # pragma: no cover
        pytest.skip(
            f"Skipping: proxy module not available ({exc}). Install 'proxy.py'."
        )

    # Launch proxy.py as a subprocess to avoid API instability between versions
    cmd = [
        sys.executable,
        "-m",
        "proxy",
        "--hostname",
        host,
        "--port",
        str(port),
        "--num-workers",
        "1",
        "--log-level",
        "WARNING",
    ]

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    try:
        try:
            _wait_for_port(host, port, timeout=10.0)
        except TimeoutError:
            # Collect logs for debugging and skip the test instead of failing hard
            try:
                stdout, stderr = proc.communicate(timeout=1)
            except Exception:
                stdout, stderr = (b"", b"")
            msg = (
                "Proxy did not start on "
                f"{host}:{port}. stderr: {stderr.decode(errors='ignore').strip()}"
            )
            pytest.skip(msg)
            return

        yield f"http://{host}:{port}"
    finally:
        if proc.poll() is None:
            try:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
            except Exception:
                proc.kill()
