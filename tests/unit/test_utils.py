from io import BytesIO
import requests
import pytest

from rich.progress import Progress
from dvuploader.file import File
from dvuploader.utils import (
    add_directory,
    build_url,
    retrieve_dataset_files,
    setup_pbar,
)


class TestAddDirectory:
    def test_all_files_added_except_hidden(self):
        # Arrange
        directory = "tests/fixtures/add_dir_files"

        # Act
        files = add_directory(directory)
        [file.extract_file_name_hash_file() for file in files]

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
            assert any(
                file.file_name == file_name for file in files
            ), f"File {file_name} not found in files"

            file = next(filter(lambda file: file.file_name == file_name, files))
            assert (
                file.directory_label == directory_label
            ), f"File {file_name} has wrong directory label"

    def test_all_files_added_except_hidden_and_dunder(self):
        # Arrange
        directory = "tests/fixtures/add_dir_files"

        # Act
        files = add_directory(directory, ignore=[r"^\.", "__.*__"])
        [file.extract_file_name_hash_file() for file in files]

        # Assert
        expected_files = [
            ("", "somefile.txt"),
            ("", "anotherfile.txt"),
            ("", "to_ignore.txt"),
            ("subdir", "subfile.txt"),
        ]

        assert len(files) == len(expected_files), "Wrong number of files"

        for directory_label, file_name in expected_files:
            assert any(
                file.file_name == file_name for file in files
            ), f"File {file_name} not found in files"

            file = next(filter(lambda file: file.file_name == file_name, files))
            assert (
                file.directory_label == directory_label
            ), f"File {file_name} has wrong directory label"


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
    # Retrieve files of a dataset with valid dataverse_url, persistent_id, and api_token.
    def test_valid_parameters_with_dotted_dict(self, mocker):
        # Mock the requests.get function
        mocker.patch("requests.get")

        # Set up mock response
        mock_response = mocker.Mock()
        mock_response.json.return_value = {
            "data": {
                "latestVersion": {
                    "files": [
                        {"file_name": "file1.txt"},
                        {"file_name": "file2.txt"},
                    ]
                }
            }
        }
        requests.get.return_value = mock_response

        # Call the function under test
        result = retrieve_dataset_files("http://example.com", "12345", "token")

        # Assert the result
        assert result == [
            {"file_name": "file1.txt"},
            {"file_name": "file2.txt"},
        ]

        # Assert that requests.get was called with the correct parameters
        requests.get.assert_called_once_with(
            "http://example.com/api/datasets/:persistentId/?persistentId=12345",
            headers={"X-Dataverse-key": "token"},
        )

    # Return a list of files in the dataset.
    def test_return_files_list(self, mocker):
        # Mock the requests.get function
        mocker.patch("requests.get")

        # Set up mock response
        mock_response = mocker.Mock()
        mock_response.json.return_value = {
            "data": {
                "latestVersion": {
                    "files": [{"file_name": "file1.txt"}, {"file_name": "file2.txt"}]
                }
            }
        }
        requests.get.return_value = mock_response

        # Call the function under test
        result = retrieve_dataset_files("http://example.com", "12345", "token")

        # Assert the result
        assert result == [{"file_name": "file1.txt"}, {"file_name": "file2.txt"}]

    # Raise HTTPError if the request to the Dataverse repository fails.
    def test_raise_http_error(self, mocker):
        # Mock the requests.get function to raise an exception
        mocker.patch("requests.get", side_effect=requests.exceptions.HTTPError)

        # Call the function under test and assert that it raises an HTTPError
        with pytest.raises(requests.exceptions.HTTPError):
            retrieve_dataset_files("http://example.com", "12345", "token")


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
