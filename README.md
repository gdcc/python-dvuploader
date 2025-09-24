<h1 align="center">
  Dataverse Uploader</br>
  <a href="https://badge.fury.io/py/dvuploader"><img src="https://badge.fury.io/py/dvuploader.svg" alt="PyPI version" height="18"></a>
  <img alt="PyPI - Python Version" src="https://img.shields.io/pypi/pyversions/dvuploader">
  <img src="https://github.com/gdcc/python-dvuploader/actions/workflows/test.yml/badge.svg" alt="Build Badge">
</h1>

Python equivalent to the [DVUploader](https://github.com/GlobalDataverseCommunityConsortium/dataverse-uploader) written in Java. Complements other libraries written in Python and facilitates the upload of files to a Dataverse instance via [Direct Upload](https://guides.dataverse.org/en/latest/developers/s3-direct-upload-api.html).

**Features**

* Parallel direct upload to a Dataverse backend storage
* Files are streamed directly instead of being buffered in memory
* Supports multipart uploads and chunks data accordingly

-----

https://github.com/gdcc/python-dvuploader/assets/30547301/671131b1-d188-4433-9f77-9ec0ed2af36e

-----

## Getting started

To get started with DVUploader, you can install it via PyPI

```bash
python3 -m pip install dvuploader
```

or by source

```bash
git clone https://github.com/gdcc/python-dvuploader.git
cd python-dvuploader
python3 -m pip install .
```

## Quickstart

### Programmatic usage

In order to perform a direct upload, you need to have a Dataverse instance running and a cloud storage provider. The following example shows how to upload files to a Dataverse instance. Simply provide the files of interest and utilize the `upload` method of a `DVUploader` instance.

```python
import dvuploader as dv


# Add file individually
files = [
    dv.File(filepath="./small.txt"),
    dv.File(filepath="./tabular.csv", tab_ingest=False),
    dv.File(directory_label="some/dir", filepath="./medium.txt"),
    dv.File(directory_label="some/dir", filepath="./big.txt"),
    *dv.add_directory("./data"), # Add an entire directory
]

DV_URL = "https://demo.dataverse.org/"
API_TOKEN = "XXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
PID = "doi:10.70122/XXX/XXXXX"

dvuploader = dv.DVUploader(files=files)
dvuploader.upload(
    api_token=API_TOKEN,
    dataverse_url=DV_URL,
    persistent_id=PID,
    n_parallel_uploads=2, # Whatever your instance can handle
)
```

### Command Line Interface

DVUploader ships with a CLI ready to use outside scripts. In order to upload files to a Dataverse instance, simply provide the files of interest, persistent identifier and credentials.

#### Using arguments

```bash
dvuploader my_file.txt my_other_file.txt \
           --pid doi:10.70122/XXX/XXXXX \
           --api-token XXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX \
           --dataverse-url https://demo.dataverse.org/ \
```

#### Using a config file

Alternatively, you can also supply a `config` file that contains all necessary information for the uploader. The `config` file is a JSON/YAML file that contains the following keys:

* `persistent_id`: Persistent identifier of the dataset to upload to.
* `dataverse_url`: URL of the Dataverse instance.
* `api_token`: API token of the Dataverse instance.
* `files`: List of files to upload. Each file is a dictionary with the following keys:
  * `filepath`: Path to the file to upload.
  * `directory_label`: Optional directory label to upload the file to.
  * `description`: Optional description of the file.
  * `mimetype`: Mimetype of the file.
  * `categories`: Optional list of categories to assign to the file.
  * `restrict`: Boolean to indicate that this is a restricted file. Defaults to False.
  * `tabIngest`: Boolean to indicate that the file should be ingested as a tab-separated file. Defaults to True.

In the following example, we upload three files to a Dataverse instance. The first file is uploaded to the root directory of the dataset, while the other two files are uploaded to the directory `some/dir`.

```yaml
# config.yml
persistent_id: doi:10.70122/XXX/XXXXX
dataverse_url: https://demo.dataverse.org/
api_token: XXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
files:
    - filepath: ./small.txt
    - filepath: ./medium.txt
      directory_label: some/dir
    - filepath: ./big.txt
      directory_label: some/dir
```

The `config` file can then be used as follows:

```bash
dvuploader --config-path config.yml
```

### Environment variables

DVUploader provides several environment variables that allow you to control retry logic and upload size limits. These can be set either through environment variables directly or programmatically using the `config` function.

**Available Environment Variables:**
- `DVUPLOADER_MAX_RETRIES`: Maximum number of retry attempts (default: 15)
- `DVUPLOADER_MAX_RETRY_TIME`: Maximum wait time between retries in seconds (default: 240)
- `DVUPLOADER_MIN_RETRY_TIME`: Minimum wait time between retries in seconds (default: 1)
- `DVUPLOADER_RETRY_MULTIPLIER`: Multiplier for exponential backoff (default: 0.1)
- `DVUPLOADER_MAX_PKG_SIZE`: Maximum package size in bytes (default: 2GB)

**Setting via environment:**
```bash
export DVUPLOADER_MAX_RETRIES=20
export DVUPLOADER_MAX_RETRY_TIME=300
export DVUPLOADER_MIN_RETRY_TIME=2
export DVUPLOADER_RETRY_MULTIPLIER=0.2
export DVUPLOADER_MAX_PKG_SIZE=3221225472  # 3GB
```

**Setting programmatically:**
```python
import dvuploader as dv

# Configure the uploader settings
dv.config(
    max_retries=20,
    max_retry_time=300,
    min_retry_time=2,
    retry_multiplier=0.2,
    max_package_size=3 * 1024**3  # 3GB
)

# Continue with your upload as normal
files = [dv.File(filepath="./data.csv")]
dvuploader = dv.DVUploader(files=files)
# ... rest of your upload code
```

The retry logic uses exponential backoff which ensures that subsequent retries will be longer, but won't exceed exceed `max_retry_time`. This is particularly useful when dealing with native uploads that may be subject to intermediate locks on the Dataverse side.

## Troubleshooting

#### `500` error and `OptimisticLockException`

When uploading multiple tabular files, you might encounter a `500` error and a `OptimisticLockException` upon the file registration step. This has been discussed in https://github.com/IQSS/dataverse/issues/11265 and is due to the fact that intermediate locks prevent the file registration step from completing.

A workaround is to set the `tabIngest` flag to `False` for all files that are to be uploaded. This will cause the files not be ingested but will avoid the intermediate locks.

```python
dv.File(filepath="tab_file.csv", tab_ingest=False)
```

Please be aware that your tabular files will not be ingested as such but will be uploaded in their native format. You can utilize [pyDataverse](https://github.com/gdcc/pyDataverse/blob/693d0ff8d2849eccc32f9e66228ee8976109881a/pyDataverse/api.py#L2475) to ingest the files after they have been uploaded.

## Development

To install the development dependencies, run the following command:

```bash
pip install poetry
poetry install --with test
```

### Running tests locally

In order to test the DVUploader, you need to have a Dataverse instance running. You can start a local Dataverse instance by following these steps:

**1. Start the Dataverse instance**

```bash
docker compose \
    -f ./docker/docker-compose-base.yml \
    --env-file local-test.env \
    up -d
```

**2. Set up the environment variables**

```bash
export BASE_URL=http://localhost:8080
export API_TOKEN=XXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
export DVUPLOADER_TESTING=true
```

**3. Run the test(s) with pytest**

```bash
poetry run pytest
```

### Linting

This repository uses `ruff` to lint the code and `codespell` to check for spelling mistakes. You can run the linters with the following command:

```bash
python -m ruff check
python -m codespell --check-filenames
```
