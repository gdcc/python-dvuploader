import yaml
import typer

from pydantic import BaseModel
from typing import List, Tuple
from dvuploader import DVUploader, File


class CliInput(BaseModel):
    api_token: str
    dataverse_url: str
    persistent_id: str
    files: List[File]


app = typer.Typer()


def _parse_yaml_config(path: str) -> Tuple[List[File], str, str, str]:
    """
    Parses a configuration file and returns a Class instance
    containing a list of File objects, a persistent ID, a Dataverse URL,
    and an API token.

    Args:
        path (str): Path to a JSON/YAML file containing specifications for the files to upload.

    Returns:
        CliInput: Class instance containing a list of File objects, a persistent ID,
                  a Dataverse URL, and an API token.

    Raises:
        ValueError: If the configuration file is invalid.
    """
    return CliInput(**yaml.safe_load(open(path)))


def _validate_inputs(
    filepaths: List[str],
    pid: str,
    dataverse_url: str,
    api_token: str,
    config_path: str,
) -> None:
    if config_path and len(filepaths) > 0:
        raise typer.BadParameter(
            "Cannot specify both a JSON/YAML file and a list of filepaths."
        )

    _has_meta_params = all(arg is not None for arg in [pid, dataverse_url, api_token])
    _has_config_file = config_path is not None

    if _has_meta_params and _has_config_file:
        print(
            "\n⚠️  Warning\n"
            "├── You have specified both a configuration file and metadata parameters via the command line.\n"
            "╰── Will use metadata parameters specified in the config file."
        )
    elif not _has_meta_params and not _has_config_file:
        raise typer.BadParameter(
            "You must specify either a JSON/YAML file or metadata parameters (dv_url, api_token, pid, files) via the command line."
        )


@app.command()
def main(
    filepaths: List[str] = typer.Argument(
        default=None,
        help="A list of filepaths to upload.",
    ),
    pid: str = typer.Option(
        default=None,
        help="The persistent identifier of the Dataverse dataset.",
    ),
    api_token: str = typer.Option(
        default=None,
        help="The API token for the Dataverse repository.",
    ),
    dataverse_url: str = typer.Option(
        default=None,
        help="The URL of the Dataverse repository.",
    ),
    config_path: str = typer.Option(
        default=None,
        help="Path to a JSON/YAML file containing specifications for the files to upload. Defaults to None.",
    ),
    n_jobs: int = typer.Option(
        default=-1,
        help="The number of parallel jobs to run. Defaults to -1.",
    ),
):
    _validate_inputs(
        filepaths=filepaths,
        pid=pid,
        dataverse_url=dataverse_url,
        api_token=api_token,
        config_path=config_path,
    )

    if config_path:
        # PyYAML is a superset of JSON, so we can use the same function to parse both
        cli_input = _parse_yaml_config(config_path)
    else:
        cli_input = CliInput(
            api_token=api_token,
            dataverse_url=dataverse_url,
            persistent_id=pid,
            files=[File(filepath=filepath) for filepath in filepaths],
        )

    uploader = DVUploader(files=cli_input.files)
    uploader.upload(
        persistent_id=cli_input.persistent_id,
        dataverse_url=cli_input.dataverse_url,
        api_token=cli_input.api_token,
        n_jobs=n_jobs,
    )


if __name__ == "__main__":
    typer.run(main)
