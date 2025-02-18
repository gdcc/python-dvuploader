from io import BytesIO, StringIO
import os
from typing import List, Optional, Union, IO

from pydantic import BaseModel, ConfigDict, Field
from pydantic.fields import PrivateAttr

from dvuploader.checksum import Checksum, ChecksumTypes


class File(BaseModel):
    """
    Represents a file with its properties and methods for uploading to Dataverse.

    Attributes:
        filepath (str): The path to the file.
        handler (Union[BytesIO, StringIO, IO, None]): File handler for reading the file contents.
        description (str): The description of the file.
        directory_label (str): The label of the directory where the file is stored.
        mimeType (str): The MIME type of the file.
        categories (List[str]): The categories associated with the file.
        restrict (bool): Indicates if the file is restricted.
        checksum_type (ChecksumTypes): The type of checksum used for the file.
        storageIdentifier (Optional[str]): The identifier of the storage where the file is stored.
        file_name (Optional[str]): The name of the file.
        checksum (Optional[Checksum]): The checksum of the file.
        to_replace (bool): Indicates if the file should be replaced.
        file_id (Optional[Union[str, int]]): The ID of the file to replace.

    Private Attributes:
        _size (int): Size of the file in bytes.

    Methods:
        extract_file_name(): Extracts filename from filepath and initializes file handler.
        _validate_filepath(path): Validates if the file path exists and is a file.
        apply_checksum(): Calculates and applies the checksum for the file.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
    )

    filepath: str = Field(..., exclude=True)
    handler: Union[BytesIO, StringIO, IO, None] = Field(default=None, exclude=True)
    description: str = ""
    directory_label: str = Field(default="", alias="directoryLabel")
    mimeType: str = "text/plain"
    categories: List[str] = ["DATA"]
    restrict: bool = False
    checksum_type: ChecksumTypes = Field(default=ChecksumTypes.MD5, exclude=True)
    storageIdentifier: Optional[str] = None
    file_name: Optional[str] = Field(default=None, alias="fileName")
    checksum: Optional[Checksum] = None
    to_replace: bool = False
    file_id: Optional[Union[str, int]] = Field(default=None, alias="fileToReplaceId")
    tab_ingest: bool = Field(default=True, alias="tabIngest")

    _size: int = PrivateAttr(default=0)

    def extract_file_name(self):
        """
        Extracts the file name from the file path and initializes the file handler.
        Also calculates the file size and prepares for checksum calculation.

        Returns:
            self: The current instance of the class.
        """

        # Hash file
        hash_algo, hash_fun = self.checksum_type.value

        if self.handler is None:
            self._validate_filepath(self.filepath)
            self.handler = open(self.filepath, "rb")
            self._size = os.path.getsize(self.filepath)
        else:
            self._size = len(self.handler.read())
            self.directory_label = os.path.dirname(self.filepath)
            self.handler.seek(0)

        if self.file_name is None:
            self.file_name = os.path.basename(self.filepath)

        self.checksum = Checksum.from_algo(
            hash_fun=hash_fun,
            hash_algo=hash_algo,
        )

        return self

    @staticmethod
    def _validate_filepath(path):
        """
        Validates if the given filepath exists and is a file.

        Args:
            path (str): The filepath to be validated.

        Raises:
            FileNotFoundError: If the filepath does not exist.
            IsADirectoryError: If the filepath points to a directory instead of a file.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"Filepath {path} does not exist.")
        elif not os.path.isfile(path):
            raise IsADirectoryError(f"Filepath {path} is not a file.")

    def apply_checksum(self):
        """
        Calculates and applies the checksum for the file.
        Must be called after extract_file_name() has initialized the checksum.

        Raises:
            AssertionError: If checksum is not initialized or hash function is not set.
        """
        assert self.checksum is not None, "Checksum is not calculated."
        assert self.checksum._hash_fun is not None, "Checksum hash function is not set."

        self.checksum.apply_checksum()

    def __del__(self):
        if self.handler is not None:
            self.handler.close()
