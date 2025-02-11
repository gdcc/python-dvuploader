from io import BytesIO
import json
import tempfile

from dvuploader.dvuploader import DVUploader
from dvuploader.file import File

from dvuploader.utils import add_directory, retrieve_dataset_files
from tests.conftest import create_dataset, create_mock_file


class TestNativeUpload:
    def test_native_upload(
        self,
        credentials,
    ):
        BASE_URL, API_TOKEN = credentials

        with tempfile.TemporaryDirectory() as directory:
            # Arrange
            create_mock_file(directory, "small_file.txt", size=1)
            create_mock_file(directory, "mid_file.txt", size=50)
            create_mock_file(directory, "large_file.txt", size=200)

            # Add all files in the directory
            files = add_directory(directory=directory)

            # Create Dataset
            pid = create_dataset(
                parent="Root",
                server_url=BASE_URL,
                api_token=API_TOKEN,
            )

            # Act
            uploader = DVUploader(files=files)
            uploader.upload(
                persistent_id=pid,
                api_token=API_TOKEN,
                dataverse_url=BASE_URL,
                n_parallel_uploads=1,
            )

            # Assert
            files = retrieve_dataset_files(
                dataverse_url=BASE_URL,
                persistent_id=pid,
                api_token=API_TOKEN,
            )

            expected_files = [
                "small_file.txt",
                "mid_file.txt",
                "large_file.txt",
            ]

            assert len(files) == 3
            assert sorted([file["label"] for file in files]) == sorted(expected_files)

    def test_forced_native_upload(
        self,
        credentials,
    ):
        BASE_URL, API_TOKEN = credentials

        with tempfile.TemporaryDirectory() as directory:
            # Arrange
            create_mock_file(directory, "small_file.txt", size=1)
            create_mock_file(directory, "mid_file.txt", size=50)
            create_mock_file(directory, "large_file.txt", size=200)

            # Add all files in the directory
            files = add_directory(directory=directory)

            # Create Dataset
            pid = create_dataset(
                parent="Root",
                server_url=BASE_URL,
                api_token=API_TOKEN,
            )

            # Act
            uploader = DVUploader(files=files)
            uploader.upload(
                persistent_id=pid,
                api_token=API_TOKEN,
                dataverse_url=BASE_URL,
                n_parallel_uploads=1,
                force_native=True,
            )

            # Assert
            files = retrieve_dataset_files(
                dataverse_url=BASE_URL,
                persistent_id=pid,
                api_token=API_TOKEN,
            )

            expected_files = [
                "small_file.txt",
                "mid_file.txt",
                "large_file.txt",
            ]

            assert len(files) == 3
            assert sorted([file["label"] for file in files]) == sorted(expected_files)

    def test_native_upload_by_handler(
        self,
        credentials,
    ):
        BASE_URL, API_TOKEN = credentials

        # Arrange
        byte_string = b"Hello, World!"
        files = [
            File(
                filepath="subdir/file.txt",
                handler=BytesIO(byte_string),
                description="This is a test",
            ),
            File(
                filepath="biggerfile.txt",
                handler=BytesIO(byte_string * 10000),
                description="This is a test",
            ),
        ]

        # Create Dataset
        pid = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
        )

        # Act
        uploader = DVUploader(files=files)
        uploader.upload(
            persistent_id=pid,
            api_token=API_TOKEN,
            dataverse_url=BASE_URL,
            n_parallel_uploads=1,
        )

        # Assert
        expected = [
            ("", "biggerfile.txt"),
            ("subdir", "file.txt"),
        ]

        files = retrieve_dataset_files(
            dataverse_url=BASE_URL,
            persistent_id=pid,
            api_token=API_TOKEN,
        )

        assert len(files) == 2

        for ex_dir, ex_f in expected:
            file = next(file for file in files if file["label"] == ex_f)

            assert file["label"] == ex_f, (
                f"File label does not match for file {json.dumps(file)}"
            )

            assert file.get("directoryLabel", "") == ex_dir, (
                f"Directory label does not match for file {json.dumps(file)}"
            )

            assert file["description"] == "This is a test", (
                f"Description does not match for file {json.dumps(file)}"
            )
