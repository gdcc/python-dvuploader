import asyncio
import os
from typing import Dict, List, Optional
from urllib.parse import urljoin

import httpx
import rich
from pydantic import BaseModel
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress
from rich.table import Table

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
        verbose (bool): Whether to print progress and status messages. Defaults to True.

    Methods:
        upload(persistent_id: str, dataverse_url: str, api_token: str, n_parallel_uploads: int = 1, force_native: bool = False, replace_existing: bool = True) -> None:
            Uploads the files to the specified Dataverse repository.
        _validate_files() -> None:
            Validates and hashes the files to be uploaded.
        _validate_file(file: File) -> None:
            Validates and hashes a single file.
        _check_duplicates(dataverse_url: str, persistent_id: str, api_token: str, replace_existing: bool) -> None:
            Checks for duplicate files in the dataset.
        _get_file_id(file: File, ds_files: List[Dict]) -> Optional[str]:
            Gets the file ID for a given file in a dataset.
        _check_hashes(file: File, dsFile: Dict) -> bool:
            Checks if a file has the same checksum as a file in the dataset.
        _has_direct_upload(dataverse_url: str, api_token: str, persistent_id: str) -> bool:
            Checks if direct upload is supported by the Dataverse instance.
        setup_progress_bars(files: List[File]) -> Tuple[Progress, List[TaskID]]:
            Sets up progress bars for tracking file uploads.
    """

    files: List[File]
    verbose: bool = True

    def upload(
        self,
        persistent_id: str,
        dataverse_url: str,
        api_token: str,
        n_parallel_uploads: int = 1,
        force_native: bool = False,
        replace_existing: bool = True,
        proxy: Optional[str] = None,
    ) -> None:
        """
        Uploads the files to the specified Dataverse repository.

        Args:
            persistent_id (str): The persistent identifier of the Dataverse dataset.
            dataverse_url (str): The URL of the Dataverse repository.
            api_token (str): The API token for the Dataverse repository.
            n_parallel_uploads (int): The number of parallel uploads to execute. For direct upload,
                this restricts parallel chunks per upload. Use n_jobs to control parallel files.
            force_native (bool): Forces the use of the native upload method instead of direct upload.
            replace_existing (bool): Whether to replace files that already exist in the dataset.
            proxy (str): The proxy to use for the upload.

        Returns:
            None
        """

        if self.verbose:
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

        if self.verbose:
            rich.print(panel)

        asyncio.run(self._validate_files())

        # Check for duplicates
        self._check_duplicates(
            dataverse_url=dataverse_url,
            persistent_id=persistent_id,
            api_token=api_token,
            replace_existing=replace_existing,
            proxy=proxy,
        )

        # Sort files by size
        files = sorted(
            self.files,
            key=lambda x: x._size,
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

        if not has_direct_upload and not force_native and self.verbose:
            rich.print(
                "\n[bold italic white]⚠️  Direct upload not supported. Falling back to Native API."
            )

        if self.verbose:
            rich.print("\n[bold italic white]🚀 Uploading files\n")

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
                        proxy=proxy,
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
                        proxy=proxy,
                    )
                )

        if self.verbose:
            rich.print("\n[bold italic white]✅ Upload complete\n")

    async def _validate_files(self):
        """
        Validates and hashes the files to be uploaded.

        Returns:
            None
        """

        tasks = [self._validate_file(file=file) for file in self.files]

        await asyncio.gather(*tasks)

    @staticmethod
    async def _validate_file(file: File):
        """
        Validates and hashes a single file.

        Args:
            file (File): The file to validate and hash.

        Returns:
            None
        """
        file.extract_file_name()

    def _check_duplicates(
        self,
        dataverse_url: str,
        persistent_id: str,
        api_token: str,
        replace_existing: bool,
        proxy: Optional[str] = None,
    ) -> None:
        """
        Checks for duplicate files in the dataset by comparing paths and filenames.

        Args:
            dataverse_url (str): The URL of the Dataverse repository.
            persistent_id (str): The persistent ID of the dataset.
            api_token (str): The API token for accessing the Dataverse repository.
            replace_existing (bool): Whether to replace files that already exist.
            proxy (Optional[str]): The proxy to use for the request.
        Returns:
            None
        """

        ds_files = retrieve_dataset_files(
            dataverse_url=dataverse_url,
            persistent_id=persistent_id,
            api_token=api_token,
            proxy=proxy,
        )

        table = Table(
            title="[bold white]🔎 Checking dataset files",
            title_justify="left",
        )
        table.add_column("File", style="cyan", no_wrap=True)
        table.add_column("Status")
        table.add_column("Action")

        over_threshold = len(self.files) > 50
        to_skip = []
        n_new_files = 0
        n_replace_files = 0

        for file in self.files:
            # If present in dataset, replace file
            file.file_id = self._get_file_id(file, ds_files)
            file.to_replace = True if file.file_id else False

            if file.to_replace:
                n_replace_files += 1
                to_skip.append(file.file_id)

                if replace_existing:
                    ds_file = self._get_dsfile_by_id(file.file_id, ds_files)  # type: ignore
                    if not self._check_size(file, ds_file):  # type: ignore
                        file._unchanged_data = False
                    else:
                        # calculate checksum
                        file.update_checksum_chunked()
                        file.apply_checksum()
                        file._unchanged_data = self._check_hashes(file, ds_file)  # type: ignore
                    if file._unchanged_data:
                        table.add_row(
                            file.file_name,
                            "[bright_cyan]Exists",
                            "[bright_black]Replace Meta",
                        )
                    else:
                        table.add_row(
                            file.file_name,
                            "[bright_cyan]Exists",
                            "[bright_black]Replace",
                        )
                else:
                    table.add_row(
                        file.file_name, "[bright_cyan]Exists", "[bright_black]Skipping"
                    )
            else:
                n_new_files += 1
                table.add_row(
                    file.file_name, "[spring_green3]New", "[bright_black]Upload"
                )

        console = Console()

        if not replace_existing:
            console.print(
                f"\nSkipping {len(to_skip)} existing files. Use `replace_existing=True` to replace them.\n"
            )
            self.files = [file for file in self.files if not file.to_replace]

        if over_threshold:
            table = Table(title="[bold white]🔎 Checking dataset files")

            table.add_column("New", style="spring_green3", no_wrap=True)
            table.add_column("Replace", style="bright_black", no_wrap=True)
            table.add_row(str(n_new_files), str(n_replace_files))

        if self.verbose:
            console.print(table)

    @staticmethod
    def _get_file_id(
        file: File,
        ds_files: List[Dict],
    ) -> Optional[str]:
        """
        Gets the file ID for a given file in a dataset.

        Args:
            file (File): The file object to find the ID for.
            ds_files (List[Dict]): List of dictionary objects representing dataset files.

        Returns:
            Optional[str]: The ID of the file if found, None otherwise.
        """

        # Find the file that matches label and directory_label
        for ds_file in ds_files:
            dspath = os.path.join(ds_file.get("directoryLabel", ""), ds_file["label"])
            fpath = os.path.join(file.directory_label, file.file_name)  # type: ignore

            if dspath == fpath:
                return ds_file["dataFile"]["id"]

    @staticmethod
    def _get_dsfile_by_id(
        file_id: int,
        ds_files: List[Dict],
    ) -> Optional[Dict]:
        """
        Retrieves a dataset file dictionary by its ID.

        Args:
            file_id (int): The ID of the file to retrieve.
            ds_files (List[Dict]): List of dictionary objects representing dataset files.

        Returns:
            Optional[Dict]: The dataset file dictionary if found, None otherwise.
        """
        for ds_file in ds_files:
            if ds_file["dataFile"]["id"] == file_id:
                return ds_file

    @staticmethod
    def _check_hashes(file: File, dsFile: Dict):
        """
        Checks if a file has the same checksum as a file in the dataset.

        Args:
            file (File): The file to check.
            dsFile (Dict): The file in the dataset to compare against.

        Returns:
            bool: True if the files have matching checksums and paths, False otherwise.
        """

        if not file.checksum:
            return False

        hash_algo, hash_value = tuple(dsFile["dataFile"]["checksum"].values())
        path = os.path.join(
            dsFile.get("directoryLabel", ""), dsFile["dataFile"]["filename"]
        )

        return (
            file.checksum.value == hash_value
            and file.checksum.type == hash_algo
            and path == os.path.join(file.directory_label, file.file_name)  # type: ignore
        )

    @staticmethod
    def _check_size(file: File, dsFile: Dict) -> bool:
        """
        Checks if the file size matches the size of the file in the dataset.

        Args:
            file (File): The file to check.
            dsFile (Dict): The file in the dataset to compare against.

        Returns:
            bool: True if the sizes match, False otherwise.
        """
        return dsFile["dataFile"]["filesize"] == file._size

    @staticmethod
    def _has_direct_upload(
        dataverse_url: str,
        api_token: str,
        persistent_id: str,
    ) -> bool:
        """
        Checks if direct upload is supported by the Dataverse instance.

        Args:
            dataverse_url (str): The URL of the Dataverse repository.
            api_token (str): The API token for the Dataverse repository.
            persistent_id (str): The persistent ID of the dataset.

        Returns:
            bool: True if direct upload is supported, False otherwise.
        """

        query = build_url(
            endpoint=urljoin(dataverse_url, TICKET_ENDPOINT),
            key=api_token,
            persistentId=persistent_id,
            size=1024,
        )

        # Send HTTP request
        response = httpx.get(query)

        if response.status_code == 404:
            return False
        else:
            return True

    def setup_progress_bars(self, files: List[File]):
        """
        Sets up progress bars for tracking file uploads.

        Args:
            files (List[File]): The list of files to create progress bars for.

        Returns:
            Tuple[Progress, List[TaskID]]: The Progress object and list of task IDs for the progress bars.
        """

        progress = Progress()
        tasks = [
            setup_pbar(
                file=file,
                progress=progress,
            )
            for file in files
        ]

        return progress, tasks
