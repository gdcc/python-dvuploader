from dvuploader.file import File
from dvuploader.nativeupload import _get_json_data


class TestNativeUpload:
    def test_get_json_data_with_tab_ingest(self):
        file = File(
            filepath="tests/fixtures/add_dir_files/somefile.txt",
            tabIngest=True,
        )
        json_data = _get_json_data(file)
        assert json_data == {
            "categories": [],
            "description": "",
            "directoryLabel": "",
            "mimeType": "application/octet-stream",
            "restrict": False,
            "tabIngest": True,
        }

    def test_get_json_data_without_tab_ingest(self):
        file = File(
            filepath="tests/fixtures/add_dir_files/somefile.txt",
            tabIngest=False,
        )

        json_data = _get_json_data(file)
        assert json_data == {
            "categories": [],
            "description": "",
            "directoryLabel": "",
            "mimeType": "application/octet-stream",
            "restrict": False,
            "tabIngest": False,
        }
