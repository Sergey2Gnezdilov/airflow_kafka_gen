from datetime import datetime
from io import BytesIO

import boto3
import pandas as pd


class S3Writer:
    def __init__(
        self, s3_config: dict, bucket_name: str = "tmp", prefix: str = "test", file_format: str = "parquet"
    ) -> None:
        self.client = boto3.client(**s3_config)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.file_format = file_format
        self.dt_format = "%Y-%m-%dT%H:%M:%S"

    def save(self, data: list[dict], dt: datetime) -> None:
        buffer = BytesIO()

        if self.file_format == "parquet":
            pd.DataFrame(data).to_parquet(buffer, compression="zstd")
        elif self.file_format == "csv.gz":
            pd.DataFrame(data).to_csv(buffer, compression="gzip")
        else:
            raise NotImplementedError

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=f"{self.prefix}/{dt.strftime(self.dt_format)}.{self.file_format}",
            Body=buffer.getvalue(),
        )
