import os
from typing import List, Optional

from pydantic import BaseModel, Field, validator

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

    @validator("fileName", always=True)
    def _extract_filename(cls, v, values):
        return os.path.basename(values["filepath"])

    @validator("checksum", always=True)
    def _calculate_hash(cls, v, values):
        fpath = values["filepath"]
        hash_algo, hash_fun = values["checksum_type"].value

        return Checksum.from_file(fpath=fpath, hash_fun=hash_fun, hash_algo=hash_algo)
