import asyncio
from io import BytesIO

import httpx
import pytest
from rich.progress import Progress

from dvuploader.file import File
from dvuploader.utils import (
    _get_dataset_id,
    add_directory,
    build_url,
    check_dataset_lock,
    retrieve_dataset_files,
    setup_pbar,
    wait_for_dataset_unlock,
)
from tests.conftest import create_dataset


class TestAddDirectory:
    def test_all_files_added_except_hidden(self):
        # Arrange
        directory = "tests/fixtures/add_dir_files"

        # Act
        files = add_directory(directory)
        [file.extract_file_name() for file in files]

        # Assert
        expected_files = [
            ("", "somefile.txt"),
            ("", "anotherfile.txt"),
            ("", "to_ignore.txt"),
            ("subdir", "subfile.txt"),
            ("__to_ignore_dir__", "subfile_in_ignore.txt"),
        ]

        assert len(files) == len(expected_files), "Wrong number of files"

        for directory_label, file_name in expected_files:
            assert any(file.file_name == file_name for file in files), (
                f"File {file_name} not found in files"
            )

            file = next(filter(lambda file: file.file_name == file_name, files))
            assert file.directory_label == directory_label, (
                f"File {file_name} has wrong directory label"
            )

    def test_all_files_added_except_hidden_and_dunder(self):
        # Arrange
        directory = "tests/fixtures/add_dir_files"

        # Act
        files = add_directory(directory, ignore=[r"^\.", "__.*__"])
        [file.extract_file_name() for file in files]

        # Assert
        expected_files = [
            ("", "somefile.txt"),
            ("", "anotherfile.txt"),
            ("", "to_ignore.txt"),
            ("subdir", "subfile.txt"),
        ]

        assert len(files) == len(expected_files), "Wrong number of files"

        for directory_label, file_name in expected_files:
            assert any(file.file_name == file_name for file in files), (
                f"File {file_name} not found in files"
            )

            file = next(filter(lambda file: file.file_name == file_name, files))
            assert file.directory_label == directory_label, (
                f"File {file_name} has wrong directory label"
            )


class TestBuildUrl:
    # Returns the endpoint if no query parameters are provided
    def test_returns_endpoint_if_no_query_parameters(self):
        # Arrange
        endpoint = "https://example.com/api"

        # Act
        result = build_url(endpoint)

        # Assert
        assert result == endpoint

    # Returns the complete URL string with query parameters if valid query parameters are provided
    def test_returns_complete_URL_with_query_parameters(self):
        # Arrange
        endpoint = "https://example.com/api"
        query_params = {"param1": "value1", "param2": "value2"}

        # Act
        result = build_url(endpoint, **query_params)

        # Assert
        assert result == "https://example.com/api?param1=value1&param2=value2"

    # Handles query parameters with integer values
    def test_handles_query_parameters_with_integer_values(self):
        # Arrange
        endpoint = "https://example.com/api"
        query_params = {"param1": 123, "param2": 456}

        # Act
        result = build_url(endpoint, **query_params)

        # Assert
        assert result == "https://example.com/api?param1=123&param2=456"

    # Returns the endpoint without a question mark if an empty dictionary is provided as query parameters
    def test_returns_endpoint_without_question_mark_if_empty_dictionary(self):
        # Arrange
        endpoint = "https://example.com/api"
        query_params = {}

        # Act
        result = build_url(endpoint, **query_params)

        # Assert
        assert result == "https://example.com/api"

    # Raises a TypeError if the endpoint is not a string
    def test_raises_TypeError_if_endpoint_not_string(self):
        # Arrange
        endpoint = 123
        query_params = {"param1": "value1", "param2": "value2"}

        # Act and Assert
        with pytest.raises(TypeError):
            build_url(endpoint, **query_params)  # type: ignore

    # Raises a TypeError if any of the query parameter keys are not strings
    def test_raises_TypeError_if_query_parameter_keys_not_strings(self):
        # Arrange
        endpoint = "https://example.com/api"
        query_params = {123: "value1", "param2": "value2"}

        # Act and Assert
        with pytest.raises(TypeError):
            build_url(endpoint, **query_params)  # type: ignore


class TestRetrieveDatasetFiles:
    # Return a list of files in the dataset.
    def test_return_files_list(self, httpx_mock):
        # Mock the requests.get function
        httpx_mock.add_response(
            url="http://example.com/api/datasets/:persistentId/?persistentId=12345",
            json={
                "data": {
                    "latestVersion": {
                        "files": [
                            {"file_name": "file1.txt"},
                            {"file_name": "file2.txt"},
                        ]
                    }
                }
            },
        )

        # Call the function under test
        result = retrieve_dataset_files("http://example.com", "12345", "token")

        # Assert the result
        assert result == [{"file_name": "file1.txt"}, {"file_name": "file2.txt"}]

    # Raise HTTPError if the request to the Dataverse repository fails.
    def test_raise_http_error(self):
        # Call the function under test and assert that it raises an HTTPError
        with pytest.raises(httpx.HTTPStatusError):
            retrieve_dataset_files("http://demo.dataverse.org", "12345", "token")


class TestSetupPbar:
    def test_returns_progress_bar_object(self):
        # Arrange
        handler = BytesIO(b"Hello, world!")
        file = File(
            filepath="test.txt",
            handler=handler,
        )

        progress = Progress()

        # Act
        result = setup_pbar(file=file, progress=progress)

        # Assert
        assert isinstance(result, int)
        assert result == 0


class TestDatasetId:
    @pytest.mark.asyncio
    async def test_get_dataset_id(self, credentials):
        # Arrange
        BASE_URL, API_TOKEN = credentials
        dataset_pid, dataset_id = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
            return_id=True,
        )

        print(dataset_pid, dataset_id)

        # Act
        async with httpx.AsyncClient(
            base_url=BASE_URL,
            headers={"X-Dataverse-key": API_TOKEN},
        ) as session:
            result = await _get_dataset_id(session=session, persistent_id=dataset_pid)

        # Assert
        assert result == dataset_id


class TestCheckDatasetLock:
    @pytest.mark.asyncio
    async def test_check_dataset_lock(self, credentials):
        # Create a dataset and apply a lock, then verify the lock is detected
        BASE_URL, API_TOKEN = credentials
        _, dataset_id = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
            return_id=True,
        )
        async with httpx.AsyncClient(
            base_url=BASE_URL,
            headers={"X-Dataverse-key": API_TOKEN},
        ) as session:
            response = await session.post(f"/api/datasets/{dataset_id}/lock/Ingest")
            response.raise_for_status()
            result = await check_dataset_lock(session=session, id=dataset_id)
            assert result is True

    @pytest.mark.asyncio
    async def test_wait_for_dataset_unlock(self, credentials):
        # Test that the unlock wait function completes when a dataset lock is released
        BASE_URL, API_TOKEN = credentials
        dataset_pid, dataset_id = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
            return_id=True,
        )
        async with httpx.AsyncClient(
            base_url=BASE_URL,
            headers={"X-Dataverse-key": API_TOKEN},
        ) as session:
            response = await session.post(f"/api/datasets/{dataset_id}/lock/Ingest")
            response.raise_for_status()

            async def release_lock():
                # Simulate background unlock after a brief pause
                await asyncio.sleep(1.5)
                unlock_resp = await session.delete(
                    f"/api/datasets/{dataset_id}/locks",
                    params={"type": "Ingest"},
                )
                unlock_resp.raise_for_status()

            release_task = asyncio.create_task(release_lock())
            await wait_for_dataset_unlock(
                session=session,
                persistent_id=dataset_pid,
                timeout=4,
            )
            await release_task  # Ensure unlock task completes

    @pytest.mark.asyncio
    async def test_wait_for_dataset_unlock_timeout(self, credentials):
        # Should raise a timeout error if dataset is not unlocked within the given window
        BASE_URL, API_TOKEN = credentials
        dataset_pid, dataset_id = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
            return_id=True,
        )
        async with httpx.AsyncClient(
            base_url=BASE_URL,
            headers={"X-Dataverse-key": API_TOKEN},
        ) as session:
            response = await session.post(f"/api/datasets/{dataset_id}/lock/Ingest")
            response.raise_for_status()

            with pytest.raises(TimeoutError):
                await wait_for_dataset_unlock(
                    session=session,
                    persistent_id=dataset_pid,
                    timeout=0.2,
                )

    @pytest.mark.asyncio
    async def test_check_dataset_lock_when_unlocked(self, credentials):
        # Confirm that check_dataset_lock returns False for unlocked datasets
        BASE_URL, API_TOKEN = credentials
        dataset_pid, dataset_id = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
            return_id=True,
        )
        async with httpx.AsyncClient(
            base_url=BASE_URL,
            headers={"X-Dataverse-key": API_TOKEN},
        ) as session:
            result = await check_dataset_lock(session=session, id=dataset_id)
            assert result is False

    @pytest.mark.asyncio
    async def test_wait_for_dataset_unlock_already_unlocked(self, credentials):
        # Wait should return promptly when there is no lock present
        BASE_URL, API_TOKEN = credentials
        dataset_pid, dataset_id = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
            return_id=True,
        )
        async with httpx.AsyncClient(
            base_url=BASE_URL,
            headers={"X-Dataverse-key": API_TOKEN},
        ) as session:
            import time

            start = time.monotonic()
            await wait_for_dataset_unlock(
                session=session,
                persistent_id=dataset_pid,
                timeout=5,
            )
            elapsed = time.monotonic() - start
            assert elapsed < 0.5  # Operation should be quick

    @pytest.mark.asyncio
    async def test_check_dataset_lock_invalid_id(self, credentials):
        # Using a likely-invalid ID should cause an HTTP error from the API
        BASE_URL, API_TOKEN = credentials
        invalid_dataset_id = 999999999

        async with httpx.AsyncClient(
            base_url=BASE_URL,
            headers={"X-Dataverse-key": API_TOKEN},
        ) as session:
            with pytest.raises(httpx.HTTPStatusError):
                await check_dataset_lock(session=session, id=invalid_dataset_id)

    @pytest.mark.asyncio
    async def test_wait_for_dataset_unlock_invalid_id(self, credentials):
        # Waiting on an invalid dataset should raise an HTTP error
        BASE_URL, API_TOKEN = credentials
        invalid_dataset_pid = "999999999"

        async with httpx.AsyncClient(
            base_url=BASE_URL,
            headers={"X-Dataverse-key": API_TOKEN},
        ) as session:
            with pytest.raises(httpx.HTTPStatusError):
                await wait_for_dataset_unlock(
                    session=session,
                    persistent_id=invalid_dataset_pid,
                    timeout=1,
                )

    @pytest.mark.asyncio
    async def test_wait_for_dataset_unlock_race_condition_at_timeout(self, credentials):
        # Test the case where unlocking occurs just before timeout
        BASE_URL, API_TOKEN = credentials
        dataset_pid, dataset_id = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
            return_id=True,
        )
        async with httpx.AsyncClient(
            base_url=BASE_URL,
            headers={"X-Dataverse-key": API_TOKEN},
        ) as session:
            response = await session.post(f"/api/datasets/{dataset_id}/lock/Ingest")
            response.raise_for_status()

            async def release_lock():
                # Unlock just before the test timeout
                await asyncio.sleep(1.8)
                unlock_resp = await session.delete(
                    f"/api/datasets/{dataset_id}/locks",
                    params={"type": "Ingest"},
                )
                unlock_resp.raise_for_status()

            release_task = asyncio.create_task(release_lock())
            await wait_for_dataset_unlock(
                session=session,
                persistent_id=dataset_pid,
                timeout=2.5,
                sleep_time=0.1,
            )
            await release_task  # Clean up after test unlock
