import grequests
import requests
import json
import os
from typing import List
from urllib.parse import urljoin

from pydantic import BaseModel
from joblib import Parallel, delayed
from dotted_dict import DottedDict

from dvuploader.directupload import direct_upload
from dvuploader.file import File


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
    ) -> None:
        """
        Uploads the files to the specified Dataverse repository in parallel.

        Args:
            persistent_id (str): The persistent identifier of the Dataverse dataset.
            dataverse_url (str): The URL of the Dataverse repository.
            api_token (str): The API token for the Dataverse repository.

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
            print("❌ No files to upload")
            return

        # Upload files in parallel
        print(f"\n🚀 Uploading files")

        Parallel(n_jobs=-1, backend="threading")(
            delayed(direct_upload)(
                file=file,
                dataverse_url=dataverse_url,
                api_token=api_token,
                persistent_id=persistent_id,
                position=position,
            )
            for position, file in enumerate(files)
        )

        print("🎉 Done!")

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

        print("🔎 Checking dataset files")

        for file in ds_files:
            hash_algo, hash_value = tuple(file.dataFile.checksum.values())
            fname = file.dataFile.filename

            same_hash = lambda file: (
                file.checksum.value == hash_value and file.checksum.type == hash_algo
            )

            if any(map(same_hash, self.files)):
                del self.files[self.files.index(next(filter(same_hash, self.files)))]
                print(
                    f"├── File '{fname}' already exists with same {hash_algo} hash - Skipping upload."
                )

        print("🎉 Done\n")

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
