import os
from typing import List

from pydantic import BaseModel
from joblib import Parallel, delayed

from dvuploader.directupload import direct_upload
from dvuploader.file import File


class DVUploder(BaseModel):
    files: List[File]

    def upload(
        self,
        persistent_id: str,
        dataverse_url: str,
        api_token: str,
    ):
        # Sort files by size
        files = sorted(
            self.files, key=lambda x: os.path.getsize(x.filepath), reverse=True
        )

        # Upload files in parallel
        print(f"\n🚀 Uploading files")

        Parallel(n_jobs=-1, backend="threading")(
            delayed(direct_upload)(
                file=file,
                dataverse_url=dataverse_url,
                api_token=api_token,
                persistent_id=persistent_id,
                position=position,
            )
            for position, file in enumerate(files)
        )

        print("🎉 Done!")
        print("\n")
