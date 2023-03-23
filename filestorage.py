import boto3

from config import c


class FileStorageClient:
    def __init__(self):
        self.client = self.set_client()

    def set_client(self):
        return boto3.client(
            "s3",
            endpoint_url=c.get("s3").get("endpoint_url"),
            aws_access_key_id=c.get("s3").get("access_key"),
            aws_secret_access_key=c.get("s3").get("secret_key"),
        )

    def get_file(self, bucket, key):
        return (
            self.client.get_object(Bucket=bucket, Key=key)["Body"]
            .read()
            .decode("utf-8")
        )


fsc = FileStorageClient()
