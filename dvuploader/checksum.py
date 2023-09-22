import hashlib
import os
from enum import Enum
from typing import Callable, List, Optional

from pydantic import BaseModel, Field, PrivateAttr, validator


class FeedbackCounter:
    """Object to provide a feedback callback keeping track of total calls."""

    def __init__(self):
        self.counter = 0

    def feedback(self, r, **kwargs):
        self.counter += 1
        print("{0} uploaded, {1} total.".format(r.url, self.counter))
        return r


class ChecksumTypes(Enum):
    SHA1 = ("SHA-1", hashlib.sha1)
    MD5 = ("MD5", hashlib.md5)
    SHA256 = ("SHA-256", hashlib.sha256)
    SHA512 = ("SHA-512", hashlib.sha512)


class Checksum(BaseModel):
    class Config:
        allow_population_by_field_name = True

    type: str = Field(..., alias="@type")
    value: str = Field(..., alias="@value")

    @classmethod
    def from_file(
        cls,
        fpath: str,
        hash_fun: Callable,
        hash_algo: str,
    ):
        """Takes a file path and returns a checksum object"""

        value = cls._chunk_checksum(fpath=fpath, hash_fun=hash_fun)
        return cls(type=hash_algo, value=value)  # type: ignore

    @staticmethod
    def _chunk_checksum(fpath: str, hash_fun: Callable, blocksize=2**20):
        """Chunks a file and returns a checksum"""

        m = hash_fun()
        with open(fpath, "rb") as f:
            while True:
                buf = f.read(blocksize)
                if not buf:
                    break
                m.update(buf)
        return m.hexdigest()
