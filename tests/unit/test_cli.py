import tempfile
from typer.testing import CliRunner
import yaml
from dvuploader.cli import _parse_yaml_config, app
from tests.conftest import create_dataset

runner = CliRunner()


class TestParseYAMLConfig:
    def test_full_input(self):
        # Assert
        fpath = "tests/fixtures/cli_input.yaml"

        # Act
        cli_input = _parse_yaml_config(fpath)
        [file.extract_file_name_hash_file() for file in cli_input.files]

        # Assert
        expected_files = [
            ("", "somefile.txt"),
            ("some/dir", "anotherfile.txt"),
        ]

        assert cli_input.api_token == "XXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
        assert cli_input.dataverse_url == "https://demo.dataverse.org/"
        assert cli_input.persistent_id == "doi:10.70122/XXX/XXXXX"

        assert len(cli_input.files) == 2
        assert sorted(
            [(file.directory_label, file.file_name) for file in cli_input.files]
        ) == sorted(expected_files)


class TestCLIMain:
    def test_kwarg_arg_input(self, credentials):
        # Arrange
        BASE_URL, API_TOKEN = credentials
        pid = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
        )

        # Act
        result = runner.invoke(
            app,
            [
                "./tests/fixtures/add_dir_files/somefile.txt",
                "--pid",
                pid,
                "--api-token",
                API_TOKEN,
                "--dataverse-url",
                BASE_URL,
            ],
        )
        assert result.exit_code == 0

    def test_yaml_input(self, credentials):
        # Arrange
        BASE_URL, API_TOKEN = credentials
        pid = create_dataset(
            parent="Root",
            server_url=BASE_URL,
            api_token=API_TOKEN,
        )

        with tempfile.NamedTemporaryFile(suffix=".yaml") as file:
            upload_params = {
                "api_token": API_TOKEN,
                "dataverse_url": BASE_URL,
                "persistent_id": pid,
                "files": [
                    {
                        "filepath": "./tests/fixtures/add_dir_files/somefile.txt",
                        "directoryLabel": "",
                    }
                ],
            }

            with open(file.name, "w") as f:
                yaml.dump(upload_params, f)

            # Act
            result = runner.invoke(app, ["--config-path", file.name])
            assert result.exit_code == 0

    def test_no_input(self):
        result = runner.invoke(app, [])
        assert result.exit_code == 2
