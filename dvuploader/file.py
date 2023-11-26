import os
from typing import List, Optional

from pydantic import BaseModel, Field, validator, ValidationError

from dvuploader.checksum import Checksum, ChecksumTypes


class File(BaseModel):
    filepath: str = Field(..., exclude=True)
    description: str = ""
    directoryLabel: str = ""
    mimeType: str = "text/plain"
    categories: List[str] = ["DATA"]
    restrict: bool = False
    checksum_type: ChecksumTypes = Field(
        default=ChecksumTypes.MD5,
        exclude=True,
    )
    storageIdentifier: Optional[str] = None
    fileName: Optional[str] = None
    checksum: Optional[Checksum] = None

    @staticmethod
    def _validate_filepath(path):
        if not os.path.exists(path):
            raise FileNotFoundError(f"Filepath {path} does not exist.")
        elif not os.path.isfile(path):
            raise TypeError(f"Filepath {path} is not a file.")
        elif not os.access(path, os.R_OK):
            raise TypeError(f"Filepath {path} is not readable.")

        return path

    @validator("fileName", always=True)
    def _extract_filename(cls, v, values):
        return os.path.basename(values["filepath"])

    @validator("checksum", always=True)
    def _calculate_hash(cls, v, values):
        cls._validate_filepath(values["filepath"])
        fpath = values["filepath"]
        hash_algo, hash_fun = values["checksum_type"].value

        return Checksum.from_file(fpath=fpath, hash_fun=hash_fun, hash_algo=hash_algo)
