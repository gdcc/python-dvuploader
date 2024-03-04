import os
from typing import List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator
import rich

from dvuploader.checksum import Checksum, ChecksumTypes


class File(BaseModel):
    """
    Represents a file with its properties and methods.

    Attributes:
        filepath (str): The path to the file.
        description (str): The description of the file.
        directoryLabel (str): The label of the directory where the file is stored.
        mimeType (str): The MIME type of the file.
        categories (List[str]): The categories associated with the file.
        restrict (bool): Indicates if the file is restricted.
        checksum_type (ChecksumTypes): The type of checksum used for the file.
        storageIdentifier (Optional[str]): The identifier of the storage where the file is stored.
        fileName (Optional[str]): The name of the file.
        checksum (Optional[Checksum]): The checksum of the file.
        to_replace (bool): Indicates if the file should be replaced.
        file_id (Optional[str]): The ID of the file.

    Methods:
        _validate_filepath(path): Validates if the file path exists and is a file.
        _extract_filename_hash_file(): Extracts the filename from the filepath and calculates the file's checksum.

    """

    model_config: ConfigDict = ConfigDict(populate_by_alias=True)

    filepath: str = Field(..., exclude=True)
    description: str = ""
    directoryLabel: str = ""
    mimeType: str = "text/plain"
    categories: List[str] = ["DATA"]
    restrict: bool = False
    checksum_type: ChecksumTypes = Field(default=ChecksumTypes.MD5, exclude=True)
    storageIdentifier: Optional[str] = None
    fileName: Optional[str] = None
    checksum: Optional[Checksum] = None
    to_replace: bool = False
    file_id: Optional[Union[str, int]] = Field(default=None, alias="fileToReplaceId")

    def extract_filename_hash_file(self):
        """
        Extracts the filename and calculates the hash of the file.

        Returns:
            self: The current instance of the class.
        """
        self._validate_filepath(self.filepath)
        self.fileName = os.path.basename(self.filepath)

        # Hash file
        hash_algo, hash_fun = self.checksum_type.value
        self.checksum = Checksum.from_file(
            fpath=self.filepath,
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
            TypeError: If the filepath is not a file.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"Filepath {path} does not exist.")
        elif not os.path.isfile(path):
            raise IsADirectoryError(f"Filepath {path} is not a file.")
