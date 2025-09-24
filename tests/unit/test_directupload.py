import httpx
import pytest
from rich.progress import Progress
from dvuploader.directupload import (
    _add_files_to_ds,
    _validate_ticket_response,
    _prepare_registration,
)

from dvuploader.file import File


class Test_AddFileToDs:
    # Should successfully add files to a Dataverse dataset with a valid file path
    @pytest.mark.asyncio
    async def test_successfully_add_file_with_valid_filepath(self, httpx_mock):
        # Mock the session.post method to return a response with status code 200
        httpx_mock.add_response(
            method="post",
            url="https://example.com/api/datasets/:persistentId/addFiles?persistentId=pid",
        )

        # Initialize the necessary variables
        session = httpx.AsyncClient()
        dataverse_url = "https://example.com"
        pid = "pid"
        fpath = "tests/fixtures/add_dir_files/somefile.txt"
        files = [File(filepath=fpath)]
        progress = Progress()
        pbar = progress.add_task("Uploading", total=1)

        # Invoke the function
        await _add_files_to_ds(
            session=session,
            dataverse_url=dataverse_url,
            pid=pid,
            files=files,
            progress=progress,
            pbar=pbar,
        )

    @pytest.mark.asyncio
    async def test_successfully_replace_file_with_valid_filepath(self, httpx_mock):
        # Mock the session.post method to return a response with status code 200
        httpx_mock.add_response(
            method="post",
            url="https://example.com/api/datasets/:persistentId/replaceFiles?persistentId=pid",
        )

        # Initialize the necessary variables
        session = httpx.AsyncClient()
        dataverse_url = "https://example.com"
        pid = "pid"
        fpath = "tests/fixtures/add_dir_files/somefile.txt"
        files = [File(filepath=fpath, to_replace=True)]
        progress = Progress()
        pbar = progress.add_task("Uploading", total=1)

        # Invoke the function
        await _add_files_to_ds(
            session=session,
            dataverse_url=dataverse_url,
            pid=pid,
            files=files,
            progress=progress,
            pbar=pbar,
        )

    @pytest.mark.asyncio
    async def test_successfully_add_and_replace_file_with_valid_filepath(
        self, httpx_mock
    ):
        # Mock the session.post method to return a response with status code 200
        httpx_mock.add_response(
            method="post",
            url="https://example.com/api/datasets/:persistentId/replaceFiles?persistentId=pid",
        )

        httpx_mock.add_response(
            method="post",
            url="https://example.com/api/datasets/:persistentId/addFiles?persistentId=pid",
        )

        # Initialize the necessary variables
        session = httpx.AsyncClient()
        dataverse_url = "https://example.com"
        pid = "pid"
        fpath = "tests/fixtures/add_dir_files/somefile.txt"
        files = [
            File(filepath=fpath, to_replace=True),
            File(filepath=fpath),
        ]
        progress = Progress()
        pbar = progress.add_task("Uploading", total=1)

        # Invoke the function
        await _add_files_to_ds(
            session=session,
            dataverse_url=dataverse_url,
            pid=pid,
            files=files,
            progress=progress,
            pbar=pbar,
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


class TestPrepareRegistration:
    def test_tab_ingest_is_set_correctly(self):
        files = [
            File(filepath="tests/fixtures/add_dir_files/somefile.txt"),
            File(
                filepath="tests/fixtures/add_dir_files/somefile.txt",
                tab_ingest=False,  # type: ignore
            ),
            File(
                filepath="tests/fixtures/add_dir_files/somefile.txt",
                restrict=True,
            ),
            File(
                filepath="tests/fixtures/add_dir_files/somefile.txt",
                categories=["Test file"],
            ),
        ]
        registration = _prepare_registration(files, use_replace=False)
        expected_registration = [
            {
                "categories": ["DATA"],
                "mimeType": "application/octet-stream",
                "restrict": False,
                "tabIngest": True,
            },
            {
                "categories": ["DATA"],
                "mimeType": "application/octet-stream",
                "restrict": False,
                "tabIngest": False,
            },
            {
                "categories": ["DATA"],
                "mimeType": "application/octet-stream",
                "restrict": True,
                "tabIngest": True,
            },
            {
                "categories": ["Test file"],
                "mimeType": "application/octet-stream",
                "restrict": False,
                "tabIngest": True,
            },
        ]
        assert registration == expected_registration
