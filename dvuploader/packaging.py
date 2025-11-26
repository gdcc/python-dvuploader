import os
import zipfile

from typing import List, Tuple
from dvuploader.file import File

MAXIMUM_PACKAGE_SIZE = int(
    os.environ.get(
        "DVUPLOADER_MAX_PKG_SIZE",
        2 * 1024**3,  # 2 GB
    )
)


def distribute_files(dv_files: List[File]) -> List[Tuple[int, List[File]]]:
    """
    Distributes a list of files into packages based on their sizes.

    Args:
        dv_files (List[File]): The list of files to be distributed.

    Returns:
        List[Tuple[int, List[File]]]: A list of tuples containing package index and list of files.
            Files are grouped into packages that don't exceed MAXIMUM_PACKAGE_SIZE.
            Files larger than MAXIMUM_PACKAGE_SIZE are placed in their own package.
    """
    packages = []
    current_package = []
    package_index = 0
    current_size = 0
    for file in dv_files:
        if file._size > MAXIMUM_PACKAGE_SIZE:
            if current_package:
                current_package, current_size, package_index = _append_and_reset(
                    (package_index, current_package),
                    packages,
                )
            current_package, current_size, package_index = _append_and_reset(
                (package_index, [file]),
                packages,
            )
            continue

        if current_size + file._size > MAXIMUM_PACKAGE_SIZE:
            if current_package:
                current_package, current_size, package_index = _append_and_reset(
                    (package_index, current_package),
                    packages,
                )

        current_package.append(file)
        current_size += file._size
    else:
        if current_package:
            _append_and_reset(
                (package_index, current_package),
                packages,
            )

    return packages


def _append_and_reset(
    package: Tuple[int, List[File]],
    packages: List[Tuple[int, List[File]]],
) -> Tuple[List[File], int, int]:
    """
    Appends the given package to the packages list and resets the package state.

    Args:
        package (Tuple[int, List[File]]): Tuple containing package index and list of files.
        packages (List[Tuple[int, List[File]]]): The list of all packages.

    Returns:
        Tuple[List[File], int, int]: Empty list for new package, reset size counter (0),
            and incremented package index.
    """
    packages.append(package)
    return [], 0, package[0] + 1


def zip_files(
    files: List[File],
    tmp_dir: str,
    index: int,
) -> str:
    """
    Creates a zip file containing the given files.

    Args:
        files (List[File]): The files to be zipped.
        tmp_dir (str): The temporary directory to store the zip file.
        index (int): Index used in the zip filename.

    Returns:
        str: The full path to the created zip file.
    """
    name = f"package_{index}.zip"
    path = os.path.join(tmp_dir, name)

    with zipfile.ZipFile(path, "w") as zip_file:
        for file in files:
            zip_file.writestr(
                data=file.handler.read(),  # type: ignore
                zinfo_or_arcname=_create_arcname(file),
            )
            file._is_inside_zip = True

    return path


def _create_arcname(file: File) -> str:
    """
    Creates the archive name (path within zip) for the given file.

    Args:
        file (File): The file to create the archive name for.

    Returns:
        str: The archive name - either just the filename, or directory_label/filename
            if directory_label is set.
    """
    if file.directory_label is not None:
        return os.path.join(file.directory_label, file.file_name)  # type: ignore
    else:
        return file.file_name
