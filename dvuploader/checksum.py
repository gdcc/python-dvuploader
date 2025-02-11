import hashlib
from enum import Enum
from types import NoneType
from typing import IO, Callable
from pydantic.fields import PrivateAttr
from typing_extensions import Optional

from pydantic import BaseModel, ConfigDict, Field


class ChecksumTypes(Enum):
    """Enum class representing different types of checksums.

    Attributes:
        SHA1: Represents the SHA-1 checksum algorithm.
        MD5: Represents the MD5 checksum algorithm.
        SHA256: Represents the SHA-256 checksum algorithm.
        SHA512: Represents the SHA-512 checksum algorithm.
    """

    SHA1 = ("SHA-1", hashlib.sha1)
    MD5 = ("MD5", hashlib.md5)
    SHA256 = ("SHA-256", hashlib.sha256)
    SHA512 = ("SHA-512", hashlib.sha512)


class Checksum(BaseModel):
    """Checksum class represents a checksum object with type and value fields.

    Attributes:
        type (str): The type of the checksum.
        value (str): The value of the checksum.
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
    )

    type: str = Field(..., alias="@type")
    value: Optional[str] = Field(None, alias="@value")
    _hash_fun = PrivateAttr(default=None)

    @classmethod
    def from_algo(
        cls,
        hash_fun: Callable,
        hash_algo: str,
    ) -> "Checksum":
        """Creates a Checksum object from a hash function and algorithm.

        Args:
            hash_fun (Callable): The hash function to use for generating the checksum.
            hash_algo (str): The hash algorithm to use for generating the checksum.

        Returns:
            Checksum: A Checksum object with type and value fields.
        """

        cls = cls(type=hash_algo, value=None)  # type: ignore
        cls._hash_fun = hash_fun()

        return cls

    def apply_checksum(self):
        """Applies the checksum to the file."""

        assert self._hash_fun is not None, "Checksum hash function is not set."

        self.value = self._hash_fun.hexdigest()

    @staticmethod
    def _chunk_checksum(handler: IO, hash_fun: Callable, blocksize=2**20) -> str:
        """Chunks a file and returns a checksum.

        Args:
            fpath (str): The file path to generate the checksum for.
            hash_fun (Callable): The hash function to use for generating the checksum.
            blocksize (int): The block size to use for reading the file.

        Returns:
            str: A string representing the checksum of the file.
        """
        m = hash_fun()
        while True:
            buf = handler.read(blocksize)

            if not isinstance(buf, bytes):
                buf = buf.encode()

            if not buf:
                break
            m.update(buf)

        handler.seek(0)

        return m.hexdigest()
