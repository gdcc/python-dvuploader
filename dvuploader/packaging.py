import os
import zipfile

from typing import List, Tuple

MAXIMUM_PACKAGE_SIZE = int(
    os.environ.get(
        "DVUPLOADER_MAX_PKG_SIZE",
        2 * 1024**3,  # 2 GB
    )
)


def distribute_files(dv_files: List["File"]):  # type: ignore
    """
    Distributes a list of files into packages based on their sizes.

    Args:
        dv_files (List[File]): The list of files to be distributed.
        maximum_size (int, optional): The maximum size of each package in bytes. Defaults to 2 * 1024**3.

    Returns:
        List[List[File]]: The distributed packages of files.
    """
    packages = []
    current_package = []
    package_index = 0
    current_size = 0
    for file in dv_files:

        if file._size > MAXIMUM_PACKAGE_SIZE:
            current_package, current_size, package_index = _append_and_reset(
                (package_index, [file]),
                packages,
            )
            continue

        if current_size + file._size > MAXIMUM_PACKAGE_SIZE:
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
    package: Tuple[int, List["File"]],  # type: ignore
    packages: List[Tuple[int, List["File"]]],  # type: ignore
):
    """
    Appends the given package to the packages list and resets the package list.

    Args:
        package (List[File]): The package to be appended.
        packages (List[List[File]]): The list of packages.

    Returns:
        Tuple[List[File], int]: The updated package list and the count of packages.
    """
    packages.append(package)
    return [], 0, package[0] + 1


def zip_files(
    files: List["File"],  # type: ignore
    tmp_dir: str,
    index: int,
):
    """
    Zips the given files into a zip file.

    Args:
        files (List[File]): The files to be zipped.
        tmp_dir (str): The temporary directory to store the zip file in.

    Returns:
        str: The path to the zip file.
    """
    path = os.path.join(tmp_dir, f"package_{index}.zip")

    with zipfile.ZipFile(path, "w") as zip_file:
        for file in files:
            zip_file.writestr(
                data=file.handler.read(),
                zinfo_or_arcname=_create_arcname(file),
            )

    return path


def _create_arcname(file: "File"):  # type: ignore
    """
    Creates the arcname for the given file.

    Args:
        file (File): The file to create the arcname for.

    Returns:
        str: The arcname for the given file.
    """
    if file.directory_label is not None:
        return os.path.join(file.directory_label, file.file_name)  # type: ignore
    else:
        return file.file_name
