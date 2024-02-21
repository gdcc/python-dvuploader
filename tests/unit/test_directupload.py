from urllib.parse import urljoin
import aiohttp
import pytest
from rich.progress import Progress
from dvuploader.directupload import (
    _add_file_to_ds,
    UPLOAD_ENDPOINT,
    REPLACE_ENDPOINT,
    _validate_ticket_response,
)

from dvuploader.file import File


class Test_AddFileToDs:
    # Should successfully add a file to a Dataverse dataset with a valid file path
    @pytest.mark.asyncio
    async def test_successfully_add_file_with_valid_filepath(self, mocker):
        # Mock the session.post method to return a response with status code 200
        mock_post = mocker.patch("aiohttp.ClientSession.post")
        mock_post.return_value.__aenter__.return_value.status = 200

        # Initialize the necessary variables
        session = aiohttp.ClientSession()
        dataverse_url = "https://example.com"
        pid = "persistent_id"
        fpath = "tests/fixtures/add_dir_files/somefile.txt"
        file = File(filepath=fpath)
        progress = Progress()
        pbar = progress.add_task("Uploading", total=1)

        # Invoke the function
        result = await _add_file_to_ds(
            session=session,
            dataverse_url=dataverse_url,
            pid=pid,
            file=file,
            progress=progress,
            pbar=pbar,
        )

        # Assert that the response status is 200 and the result is True
        assert mock_post.called_with(
            urljoin(dataverse_url, UPLOAD_ENDPOINT + pid), data=mocker.ANY
        )

    @pytest.mark.asyncio
    async def test_successfully_replace_file_with_valid_filepath(self, mocker):
        # Mock the session.post method to return a response with status code 200
        mock_post = mocker.patch("aiohttp.ClientSession.post")
        mock_post.return_value.__aenter__.return_value.status = 200

        # Initialize the necessary variables
        session = aiohttp.ClientSession()
        dataverse_url = "https://example.com"
        pid = "persistent_id"
        fpath = "tests/fixtures/add_dir_files/somefile.txt"
        file = File(filepath=fpath, file_id="0")
        progress = Progress()
        pbar = progress.add_task("Uploading", total=1)

        # Invoke the function
        result = await _add_file_to_ds(
            session=session,
            dataverse_url=dataverse_url,
            pid=pid,
            file=file,
            progress=progress,
            pbar=pbar,
        )

        # Assert that the response status is 200 and the result is True
        assert mock_post.called_with(
            urljoin(dataverse_url, REPLACE_ENDPOINT.format(FILE_ID=file.file_id)),
            data=mocker.ANY,
        )


class Test_ValidateTicketResponse:
    # Function does not raise any exceptions when all necessary fields are present
    def test_no_exceptions_when_fields_present(self):
        response = {
            "abort": "abort_url",
            "complete": "complete_url",
            "partSize": 100,
            "urls": {"url1": "url1", "url2": "url2"},
            "storageIdentifier": "storage_id",
        }
        try:
            _validate_ticket_response(response)
        except AssertionError:
            pytest.fail("AssertionError raised when all necessary fields are present")

    # Function raises AssertionError when 'abort' field is missing
    def test_raises_assertion_error_when_abort_field_missing(self):
        response = {
            "complete": "complete_url",
            "partSize": 100,
            "urls": {"url1": "url1", "url2": "url2"},
            "storageIdentifier": "storage_id",
        }
        with pytest.raises(AssertionError):
            _validate_ticket_response(response)
