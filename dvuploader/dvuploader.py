import asyncio
from urllib.parse import urljoin
import requests
import os
import rich
from typing import Dict, List, Optional

from pydantic import BaseModel
from rich.progress import Progress, TaskID
from rich.table import Table
from rich.console import Console
from rich.panel import Panel

from dvuploader.directupload import (
    TICKET_ENDPOINT,
    direct_upload,
)
from dvuploader.file import File
from dvuploader.nativeupload import native_upload
from dvuploader.utils import build_url, retrieve_dataset_files, setup_pbar


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
        n_parallel_uploads: int = 1,
        force_native: bool = False,
    ) -> None:
        """
        Uploads the files to the specified Dataverse repository in parallel.

        Args:
            persistent_id (str): The persistent identifier of the Dataverse dataset.
            dataverse_url (str): The URL of the Dataverse repository.
            api_token (str): The API token for the Dataverse repository.
            n_parallel_uploads (int): The number of parallel uploads to execute. In the case of direct upload, this restricts the amount of parallel chunks per upload. Please use n_jobs to control parallel files.

        Returns:
            None
        """

        print("\n")
        info = "\n".join(
            [
                f"Server: [bold]{dataverse_url}[/bold]",  # type: ignore
                f"PID: [bold]{persistent_id}[/bold]",  # type: ignore
                f"Files: {len(self.files)}",
            ]
        )

        panel = Panel(
            info,
            title="[bold]DVUploader[/bold]",
            expand=False,
        )

        rich.print(panel)

        asyncio.run(self._validate_and_hash_files())

        # Check for duplicates
        self._check_duplicates(
            dataverse_url=dataverse_url,
            persistent_id=persistent_id,
            api_token=api_token,
        )

        # Sort files by size
        files = sorted(
            self.files,
            key=lambda x: os.path.getsize(x.filepath),
            reverse=False,
        )

        if not self.files:
            rich.print("\n[bold italic white]❌ No files to upload\n")
            return

        # Check if direct upload is supported
        has_direct_upload = self._has_direct_upload(
            dataverse_url=dataverse_url,
            api_token=api_token,
            persistent_id=persistent_id,
        )

        if not has_direct_upload and not force_native:
            rich.print(
                "\n[bold italic white]⚠️  Direct upload not supported. Falling back to Native API."
            )

        rich.print(f"\n[bold italic white]🚀 Uploading files\n")

        progress, pbars = self.setup_progress_bars(files=files)

        if not has_direct_upload or force_native:
            with progress:
                asyncio.run(
                    native_upload(
                        files=files,
                        dataverse_url=dataverse_url,
                        api_token=api_token,
                        persistent_id=persistent_id,
                        n_parallel_uploads=n_parallel_uploads,
                        progress=progress,
                        pbars=pbars,
                    )
                )
        else:
            with progress:
                asyncio.run(
                    direct_upload(
                        files=files,
                        dataverse_url=dataverse_url,
                        api_token=api_token,
                        persistent_id=persistent_id,
                        pbars=pbars,
                        progress=progress,
                        n_parallel_uploads=n_parallel_uploads,
                    )
                )

        rich.print("\n[bold italic white]✅ Upload complete\n")

    async def _validate_and_hash_files(self):
        """
        Validates and hashes the files to be uploaded.

        Returns:
            None
        """

        print("\n")

        progress = Progress()
        task = progress.add_task(
            "[bold italic white]📦 Preparing upload[/bold italic white]",
            total=len(self.files),
        )
        with progress:
            tasks = [
                self._validate_and_hash_file(
                    file=file,
                    progress=progress,
                    task_id=task,
                )
                for file in self.files
            ]

            await asyncio.gather(*tasks)

        print("\n")

    @staticmethod
    async def _validate_and_hash_file(
        file: File,
        progress: Progress,
        task_id: TaskID,
    ):
        file.extract_filename_hash_file()
        progress.update(task_id, advance=1)

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

        ds_files = retrieve_dataset_files(
            dataverse_url=dataverse_url,
            persistent_id=persistent_id,
            api_token=api_token,
        )

        table = Table(
            title="[bold white]🔎 Checking dataset files",
            title_justify="left",
        )
        table.add_column("File", style="cyan", no_wrap=True)
        table.add_column("Status")
        table.add_column("Action")

        to_remove = []
        over_threshold = len(self.files) > 50
        n_new_files = 0
        n_skip_files = 0

        for file in self.files:
            has_same_hash = any(
                map(lambda dsFile: self._check_hashes(file, dsFile), ds_files)
            )

            if has_same_hash and file.checksum:
                n_skip_files += 1
                table.add_row(
                    file.fileName, "[bright_black]Same hash", "[bright_black]Skip"
                )
                to_remove.append(file)
            else:
                n_new_files += 1
                table.add_row(
                    file.fileName, "[spring_green3]New", "[spring_green3]Upload"
                )

                # If present in dataset, replace file
                file.file_id = self._get_file_id(file, ds_files)
                file.to_replace = True if file.file_id else False

        for file in to_remove:
            self.files.remove(file)

        console = Console()

        if over_threshold:
            table = Table(title="[bold white]🔎 Checking dataset files")

            table.add_column("New", style="spring_green3", no_wrap=True)
            table.add_column("Skipped", style="bright_black", no_wrap=True)
            table.add_row(str(n_new_files), str(n_skip_files))

        console.print(table)

    @staticmethod
    def _get_file_id(
        file: File,
        ds_files: List[Dict],
    ) -> Optional[str]:
        """
        Get the file ID for a given file in a dataset.

        Args:
            file (File): The file object to find the ID for.
            ds_files (List[Dict]): List of dictionary objects representing dataset files.
            persistent_id (str): The persistent ID of the dataset.

        Returns:
            str: The ID of the file.

        Raises:
            ValueError: If the file cannot be found in the dataset.
        """

        # Find the file that matches label and directoryLabel
        for ds_file in ds_files:
            dspath = os.path.join(ds_file.get("directoryLabel", ""), ds_file["label"])
            fpath = os.path.join(file.directoryLabel, file.fileName)  # type: ignore

            if dspath == fpath:
                return ds_file["dataFile"]["id"]

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

        if not file.checksum:
            return False

        hash_algo, hash_value = tuple(dsFile["dataFile"]["checksum"].values())

        return (
            file.checksum.value == hash_value
            and file.checksum.type == hash_algo
            and file.fileName == dsFile["label"]
            and file.directoryLabel == dsFile.get("directoryLabel", "")
        )

    @staticmethod
    def _has_direct_upload(
        dataverse_url: str,
        api_token: str,
        persistent_id: str,
    ) -> bool:
        """Checks if the response from the ticket request contains a direct upload URL"""

        query = build_url(
            endpoint=urljoin(dataverse_url, TICKET_ENDPOINT),
            key=api_token,
            persistentId=persistent_id,
            size=1024,
        )

        # Send HTTP request
        response = requests.get(query)

        if response.status_code == 404:
            return False
        else:
            return True

    def setup_progress_bars(self, files: List[File]):
        """
        Sets up progress bars for each file in the uploader.

        Returns:
            A list of progress bars, one for each file in the uploader.
        """

        progress = Progress()
        tasks = [
            setup_pbar(
                fpath=file.filepath,
                progress=progress,
            )
            for file in files
        ]

        return progress, tasks
