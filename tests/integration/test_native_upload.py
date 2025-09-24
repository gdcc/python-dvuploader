from io import BytesIO
import json
import os
import tempfile

import pytest

from dvuploader.dvuploader import DVUploader
from dvuploader.file import File

from dvuploader.utils import add_directory, retrieve_dataset_files
from tests.conftest import create_dataset, create_mock_file, create_mock_tabular_file


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

    def test_native_upload_with_proxy(
        self,
        credentials,
    ):
        BASE_URL, API_TOKEN = credentials
        proxy = "http://127.0.0.1:3128"

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
                proxy=proxy,
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
                f"File label {ex_f} does not match for file {json.dumps(file, indent=2)}"
            )

            assert file.get("directoryLabel", "") == ex_dir, (
                f"Directory label '{ex_dir}' of expected file '{ex_f}' does not match for file {json.dumps(file, indent=2)}"
            )

            assert file["description"] == "This is a test", (
                f"Description does not match for file {json.dumps(file)}"
            )

    def test_native_upload_with_large_tabular_files_loop(
        self,
        credentials,
    ):
        BASE_URL, API_TOKEN = credentials

        # Create Dataset
        pid = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
        )

        # We are uploading large tabular files in a loop to test the uploader's
        # ability to wait for locks to be released.
        #
        # The uploader should wait for the lock to be released and then upload
        # the file.
        #
        rows = os.environ.get("TEST_ROWS", 10000)

        try:
            rows = int(rows)
        except ValueError:
            raise ValueError(f"TEST_ROWS must be an integer, got {rows}")

        # We first try the sequential case by uploading 10 files in a loop.
        with tempfile.TemporaryDirectory() as directory:
            for i in range(10):
                # Arrange
                path = create_mock_tabular_file(
                    directory,
                    f"large_tabular_file_{i}.csv",
                    rows=rows,
                    cols=20,
                )

                # Add all files in the directory
                files = [File(filepath=path)]

                # Act
                uploader = DVUploader(files=files)
                uploader.upload(
                    persistent_id=pid,
                    api_token=API_TOKEN,
                    dataverse_url=BASE_URL,
                    n_parallel_uploads=1,
                )

    def test_native_upload_with_large_tabular_files(
        self,
        credentials,
    ):
        BASE_URL, API_TOKEN = credentials

        # Create Dataset
        pid = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
        )

        # We are uploading large tabular files in a loop to test the uploader's
        # ability to wait for locks to be released.
        #
        # The uploader should wait for the lock to be released and then upload
        # the file.
        #
        rows = os.environ.get("TEST_ROWS", 10000)

        try:
            rows = int(rows)
        except ValueError:
            raise ValueError(f"TEST_ROWS must be an integer, got {rows}")

        # We first try the sequential case by uploading 10 files in a loop.
        with tempfile.TemporaryDirectory() as directory:
            files = []
            for i in range(10):
                # Arrange
                path = create_mock_tabular_file(
                    directory,
                    f"large_tabular_file_{i}.csv",
                    rows=rows,
                    cols=20,
                )

                # Add all files in the directory
                files.append(File(filepath=path))

            # Act
            uploader = DVUploader(files=files)
            uploader.upload(
                persistent_id=pid,
                api_token=API_TOKEN,
                dataverse_url=BASE_URL,
                n_parallel_uploads=1,
            )

    def test_native_upload_with_large_tabular_files_parallel(
        self,
        credentials,
    ):
        BASE_URL, API_TOKEN = credentials

        # Create Dataset
        pid = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
        )

        # We are uploading large tabular files in a loop to test the uploader's
        # ability to wait for locks to be released.
        #
        # The uploader should wait for the lock to be released and then upload
        # the file.
        #
        rows = os.environ.get("TEST_ROWS", 10000)

        try:
            rows = int(rows)
        except ValueError:
            raise ValueError(f"TEST_ROWS must be an integer, got {rows}")

        # We first try the sequential case by uploading 10 files in a loop.
        with tempfile.TemporaryDirectory() as directory:
            files = []
            for i in range(10):
                # Arrange
                path = create_mock_tabular_file(
                    directory,
                    f"large_tabular_file_{i}.csv",
                    rows=rows,
                    cols=20,
                )

                # Add all files in the directory
                files.append(File(filepath=path))

            # Act
            uploader = DVUploader(files=files)
            uploader.upload(
                persistent_id=pid,
                api_token=API_TOKEN,
                dataverse_url=BASE_URL,
                n_parallel_uploads=10,
            )

    def test_zip_file_upload(
        self,
        credentials,
    ):
        BASE_URL, API_TOKEN = credentials

        # Create Dataset
        pid = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
        )

        # Arrange
        files = [
            File(filepath="tests/fixtures/archive.zip"),
        ]

        # Act
        uploader = DVUploader(files=files)
        uploader.upload(
            persistent_id=pid,
            api_token=API_TOKEN,
            dataverse_url=BASE_URL,
            n_parallel_uploads=10,
        )

        # Assert
        files = retrieve_dataset_files(
            dataverse_url=BASE_URL,
            persistent_id=pid,
            api_token=API_TOKEN,
        )

        assert len(files) == 5, f"Expected 5 files, got {len(files)}"

        expected_files = [
            "hallo.tab",
            "hallo2.tab",
            "hallo3.tab",
            "hallo4.tab",
            "hallo5.tab",
        ]

        assert sorted([file["label"] for file in files]) == sorted(expected_files)

    def test_zipzip_file_upload(
        self,
        credentials,
    ):
        BASE_URL, API_TOKEN = credentials

        # Create Dataset
        pid = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
        )

        # Arrange
        files = [
            File(filepath="tests/fixtures/archive.zip.zip"),
        ]

        # Act
        uploader = DVUploader(files=files)
        uploader.upload(
            persistent_id=pid,
            api_token=API_TOKEN,
            dataverse_url=BASE_URL,
            n_parallel_uploads=10,
        )

        # Assert
        files = retrieve_dataset_files(
            dataverse_url=BASE_URL,
            persistent_id=pid,
            api_token=API_TOKEN,
        )

        assert len(files) == 1, f"Expected 1 file, got {len(files)}"

        expected_files = [
            "Archiv.zip",  # codespell:ignore
        ]

        assert sorted([file["label"] for file in files]) == sorted(expected_files)

    def test_metadata_with_zip_files_in_package(self, credentials):
        BASE_URL, API_TOKEN = credentials

        # Create Dataset
        pid = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
        )

        # Arrange
        files = [
            File(filepath="tests/fixtures/archive.zip",
                  dv_dir="subdir2",
                  description="This file should not be unzipped",
                  categories=["Test file"]
            ),
            File(filepath="tests/fixtures/add_dir_files/somefile.txt",
                  dv_dir="subdir",
                  description="A simple text file",
                  categories=["Test file"]
            ),
        ]

        # Act
        uploader = DVUploader(files=files)
        uploader.upload(
            persistent_id=pid,
            api_token=API_TOKEN,
            dataverse_url=BASE_URL,
            n_parallel_uploads=10,
        )

        # Assert
        files = retrieve_dataset_files(
            dataverse_url=BASE_URL,
            persistent_id=pid,
            api_token=API_TOKEN,
        )

        assert len(files) == 2, f"Expected 2 files, got {len(files)}"

        expected_files = [
            {
                "label": "archive.zip",
                "description": "This file should not be unzipped",
                "categories": ["Test file"]
            },
            {
                "label": "somefile.txt",
                "description": "A simple text file",
                "categories": ["Test file"]
            },
        ]

        files_as_expected = sorted(
            [
                {
                    k: (f[k] if k in f else None)
                    for k in expected_files[0].keys()
                }
                for f in files
            ],
            key=lambda x: x["label"]
        )
        assert files_as_expected == expected_files, (
            f"File metadata not as expected: {json.dumps(files, indent=2)}"
        )


    def test_too_many_zip_files(
        self,
        credentials,
    ):
        BASE_URL, API_TOKEN = credentials

        # Create Dataset
        pid = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
        )

        # Arrange
        files = [
            File(filepath="tests/fixtures/many_files.zip"),
        ]

        # Act
        uploader = DVUploader(files=files)

        with pytest.raises(ValueError):
            uploader.upload(
                persistent_id=pid,
                api_token=API_TOKEN,
                dataverse_url=BASE_URL,
                n_parallel_uploads=10,
            )
