import grequests
import requests
import json
import os
from typing import Dict, List
from urllib.parse import urljoin

from pydantic import BaseModel
from joblib import Parallel, delayed
from dotted_dict import DottedDict

from dvuploader.directupload import (
    TICKET_ENDPOINT,
    _abort_upload,
    _validate_ticket_response,
    direct_upload,
)
from dvuploader.file import File
from dvuploader.nativeupload import native_upload
from dvuploader.utils import build_url


class DVUploader(BaseModel):
    """
    A class for uploading files to a Dataverse repository.

    Attributes:
        files (List[File]): A list of File objects to be uploaded.

    Methods:
        upload(persistent_id: str, dataverse_url: str, api_token: str) -> None:
            Uploads the files to the specified Dataverse repository in parallel.

    """

    files: List[File]

    def upload(
        self,
        persistent_id: str,
        dataverse_url: str,
        api_token: str,
        n_jobs: int = -1,
        n_paralell_uploads: int = 1,
    ) -> None:
        """
        Uploads the files to the specified Dataverse repository in parallel.

        Args:
            persistent_id (str): The persistent identifier of the Dataverse dataset.
            dataverse_url (str): The URL of the Dataverse repository.
            api_token (str): The API token for the Dataverse repository.
            n_jobs (int): The number of parallel jobs to run. Defaults to -1.

        Returns:
            None
        """

        # Check for duplicates
        self._check_duplicates(
            dataverse_url=dataverse_url,
            persistent_id=persistent_id,
            api_token=api_token,
        )

        # Sort files by size
        files = sorted(
            self.files, key=lambda x: os.path.getsize(x.filepath), reverse=True
        )

        if not self.files:
            print("\n❌ No files to upload\n")
            return

        # Check if direct upload is supported
        has_direct_upload = self._has_direct_upload(
            dataverse_url=dataverse_url,
            api_token=api_token,
            persistent_id=persistent_id,
        )
        print("\n⚠️  Direct upload not supported. Falling back to Native API.")

        print(f"\n🚀 Uploading files")

        if not has_direct_upload:
            self._execute_native_uploads(
                files=files,
                dataverse_url=dataverse_url,
                api_token=api_token,
                persistent_id=persistent_id,
                n_paralell_uploads=n_paralell_uploads,
            )
        else:
            self._parallel_direct_upload(
                files=files,
                dataverse_url=dataverse_url,
                api_token=api_token,
                persistent_id=persistent_id,
                n_jobs=n_jobs,
            )

        print("🎉 Done!\n")

    def _check_duplicates(
        self,
        dataverse_url: str,
        persistent_id: str,
        api_token: str,
    ):
        """
        Checks for duplicate files in the dataset by comparing the checksums.

        Parameters:
            dataverse_url (str): The URL of the dataverse.
            persistent_id (str): The persistent ID of the dataset.
            api_token (str): The API token for accessing the dataverse.

        Prints a message for each file that already exists in the dataset with the same checksum.
        """

        ds_files = self._retrieve_dataset_files(
            dataverse_url=dataverse_url,
            persistent_id=persistent_id,
            api_token=api_token,
        )

        print("\n🔎 Checking dataset files")

        to_remove = []

        for file in self.files:
            if any(map(lambda dsFile: self._check_hashes(file, dsFile), ds_files)):
                print(
                    f"├── File '{file.fileName}' already exists with same {file.checksum.type} hash - Skipping upload."
                )
                to_remove.append(file)
            else:
                print(f"├── File '{file.fileName}' is new - Uploading.")

        for file in to_remove:
            self.files.remove(file)

        print("🎉 Done!")

    @staticmethod
    def _check_hashes(file: File, dsFile: Dict):
        """
        Checks if a file has the same checksum as a file in the dataset.

        Parameters:
            file (File): The file to check.
            dsFile (Dict): The file in the dataset to compare to.

        Returns:
            bool: True if the files have the same checksum, False otherwise.
        """

        hash_algo, hash_value = tuple(dsFile.dataFile.checksum.values())

        return file.checksum.value == hash_value and file.checksum.type == hash_algo

    @staticmethod
    def _retrieve_dataset_files(
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
            )

        return DottedDict(response.json()).data.latestVersion.files

    @staticmethod
    def _has_direct_upload(
        dataverse_url: str,
        api_token: str,
        persistent_id: str,
    ) -> bool:
        """Checks if the response from the ticket request contains a direct upload URL"""

        query = build_url(
            endpoint=TICKET_ENDPOINT,
            dataverse_url=dataverse_url,
            key=api_token,
            persistentId=persistent_id,
            size=1024,
        )

        # Send HTTP request
        response = requests.get(query).json()
        expected_error = "Direct upload not supported for files in this dataset"

        if "message" in response and expected_error in response["message"]:
            return False

        # Abort test upload for now, if direct upload is supported
        data = DottedDict(response.json()["data"])
        _validate_ticket_response(data)
        _abort_upload(
            data.abort,
            dataverse_url,
            api_token,
        )

    @staticmethod
    def _execute_native_uploads(
        files: List[File],
        dataverse_url: str,
        api_token: str,
        persistent_id: str,
        n_paralell_uploads: int,
    ) -> List[requests.Response]:
        """
        Executes native uploads for the given files in parallel.

        Args:
            files (List[File]): The list of File objects to be uploaded.
            dataverse_url (str): The URL of the Dataverse repository.
            api_token (str): The API token for the Dataverse repository.
            persistent_id (str): The persistent identifier of the Dataverse dataset.
            n_paralell_uploads (int): The number of parallel uploads to execute.

        Returns:
            List[requests.Response]: The list of responses for each file upload.
        """

        tasks = [
            native_upload(
                file=file,
                dataverse_url=dataverse_url,
                api_token=api_token,
                persistent_id=persistent_id,
                position=position,
            )
            for position, file in enumerate(files)
        ]

        # Execute tasks
        responses = grequests.map(tasks, size=n_paralell_uploads)

        if not all(map(lambda x: x.status_code == 200, responses)):
            errors = "\n".join(
                ["\n\n❌ Failed to upload files:"]
                + [
                    f"├── File '{file.fileName}' could not be uploaded: {response.status_code} {response.json()['message']}"
                    for file, response in zip(files, responses)
                    if response.status_code != 200
                ]
            )

            print(errors, "\n")

    @staticmethod
    def _parallel_direct_upload(
        files: List[File],
        dataverse_url: str,
        api_token: str,
        persistent_id: str,
        n_jobs: int = -1,
    ) -> None:
        """
        Perform parallel direct upload of files to the specified Dataverse repository.

        Args:
            files (List[File]): A list of File objects to be uploaded.
            dataverse_url (str): The URL of the Dataverse repository.
            api_token (str): The API token for the Dataverse repository.
            persistent_id (str): The persistent identifier of the Dataverse dataset.
            n_jobs (int): The number of parallel jobs to run. Defaults to -1.

        Returns:
            None
        """

        Parallel(n_jobs=n_jobs, backend="threading")(
            delayed(direct_upload)(
                file=file,
                dataverse_url=dataverse_url,
                api_token=api_token,
                persistent_id=persistent_id,
                position=position,
            )
            for position, file in enumerate(files)
        )
