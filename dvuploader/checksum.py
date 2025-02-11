import hashlib
from enum import Enum
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
    """Class for calculating and storing file checksums.

    This class handles checksum calculation and storage for files being uploaded to Dataverse.
    It supports multiple hash algorithms through the ChecksumTypes enum.

    Attributes:
        type (str): The type of checksum algorithm being used (e.g. "SHA-1", "MD5")
        value (Optional[str]): The calculated checksum value, or None if not yet calculated
        _hash_fun (PrivateAttr): Internal hash function instance used for calculation
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
        """Creates a new Checksum instance configured for a specific hash algorithm.

        Args:
            hash_fun (Callable): Hash function constructor (e.g. hashlib.sha1)
            hash_algo (str): Name of the hash algorithm (e.g. "SHA-1")

        Returns:
            Checksum: A new Checksum instance ready for calculating checksums
        """

        cls = cls(type=hash_algo, value=None)  # type: ignore
        cls._hash_fun = hash_fun()

        return cls

    def apply_checksum(self):
        """Finalizes and stores the calculated checksum value.

        This should be called after all data has been processed through the hash function.
        The resulting checksum is stored in the value attribute.

        Raises:
            AssertionError: If the hash function has not been initialized
        """

        assert self._hash_fun is not None, "Checksum hash function is not set."

        self.value = self._hash_fun.hexdigest()

    @staticmethod
    def _chunk_checksum(handler: IO, hash_fun: Callable, blocksize=2**20) -> str:
        """Calculates a file's checksum by processing it in chunks.

        Args:
            handler (IO): File-like object to read data from
            hash_fun (Callable): Hash function constructor to use
            blocksize (int, optional): Size of chunks to read. Defaults to 1MB (2**20)

        Returns:
            str: Hexadecimal string representation of the calculated checksum

        Note:
            This method resets the file position to the start after reading
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
