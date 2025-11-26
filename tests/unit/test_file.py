import pytest

from dvuploader.file import File


class TestFile:
    def test_read_file(self):
        # Arrange
        fpath = "tests/fixtures/add_dir_files/somefile.txt"

        # Act
        file = File(
            filepath=fpath,
            directoryLabel="",
        )

        file.extract_file_name()

        # Assert
        assert file.file_name == "somefile.txt"

    def test_read_non_existent_file(self):
        # Arrange
        fpath = "tests/fixtures/add_dir_files/non_existent.txt"

        # Act
        with pytest.raises(FileNotFoundError):
            file = File(
                filepath=fpath,
                directoryLabel="",
            )

            file.extract_file_name()

    def test_read_non_file(self):
        # Arrange
        fpath = "tests/fixtures/add_dir_files"

        # Act
        with pytest.raises(IsADirectoryError):
            file = File(
                filepath=fpath,
                directoryLabel="",
            )

            file.extract_file_name()
