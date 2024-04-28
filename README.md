<h1 align="center">
  Dataverse Uploader</br>
  <a href="https://badge.fury.io/py/dvuploader"><img src="https://badge.fury.io/py/dvuploader.svg" alt="PyPI version" height="18"></a>
  <img src="https://img.shields.io/badge/python-3.8 | 3.9 | 3.10 | 3.11-blue.svg" alt="Build Badge">
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

Alternatively, you can also supply a `config` file that contains all necessary informations for the uploader. The `config` file is a JSON/YAML file that contains the following keys:

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
