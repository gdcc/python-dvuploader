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
        categories (Optional[List[str]]): The categories associated with the file.
        restrict (bool): Indicates if the file is restricted.
        checksum_type (ChecksumTypes): The type of checksum used for the file.
        storageIdentifier (Optional[str]): The identifier of the storage where the file is stored.
        file_name (Optional[str]): The name of the file.
        checksum (Optional[Checksum]): The checksum of the file.
        to_replace (bool): Indicates if the file should be replaced.
        file_id (Optional[Union[str, int]]): The ID of the file to replace.

    Private Attributes:
        _size (int): Size of the file in bytes.
        _unchanged_data (bool): Indicates if the file data has not changed since last upload.
        _enforce_metadata_update (bool): Indicates if metadata update is enforced.
        _is_inside_zip (bool): Indicates if the file is packaged inside a zip archive.

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
    mimeType: str = "application/octet-stream"
    categories: Optional[List[str]] = ["DATA"]
    restrict: bool = False
    checksum_type: ChecksumTypes = Field(default=ChecksumTypes.MD5, exclude=True)
    storageIdentifier: Optional[str] = None
    file_name: Optional[str] = Field(default=None, alias="fileName")
    checksum: Optional[Checksum] = None
    to_replace: bool = False
    file_id: Optional[Union[str, int]] = Field(default=None, alias="fileToReplaceId")
    tab_ingest: bool = Field(default=True, alias="tabIngest")

    _size: int = PrivateAttr(default=0)
    _unchanged_data: bool = PrivateAttr(default=False)
    _enforce_metadata_update: bool = PrivateAttr(default=False)
    _is_inside_zip: bool = PrivateAttr(default=False)

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

    def get_handler(self) -> IO:
        """
        Opens the file and initializes the file handler.
        """
        if self.handler is not None:
            return self.handler

        return open(self.filepath, "rb")

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
        And all data has been fed into the checksum hash function.

        Raises:
            AssertionError: If checksum is not initialized or hash function is not set.
        """
        assert self.checksum is not None, "Checksum is not calculated."
        assert self.checksum._hash_fun is not None, "Checksum hash function is not set."

        self.checksum.apply_checksum()

    def update_checksum_chunked(self, blocksize=2**20):
        """Updates the checksum with data read from a file-like object in chunks.

        Args:
            blocksize (int, optional): Size of chunks to read. Defaults to 1MB (2**20)

        Raises:
            AssertionError: If the hash function has not been initialized

        Note:
            This method resets the file position to the start after reading.
        """
        assert self.checksum is not None, "Checksum is not initialized."
        assert self.checksum._hash_fun is not None, "Checksum hash function is not set."

        handler = self.get_handler()

        while True:
            buf = handler.read(blocksize)

            if not isinstance(buf, bytes):
                buf = buf.encode()

            if not buf:
                break
            self.checksum._hash_fun.update(buf)

        if self.handler is not None:  # type: ignore
            # In case of passed handler, we need to seek the handler to the start after reading.
            self.handler.seek(0)
        else:
            # Path-based handlers will be opened just-in-time, so we can close it.
            handler.close()

    def __del__(self):
        if self.handler is not None:
            self.handler.close()
