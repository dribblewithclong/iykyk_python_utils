import json
import boto3


class S3:
    def __init__(
        self,
        access_key: str,
        secret_key: str,
        endpoint_url: str,
    ):
        self.client = boto3.client(
            service_name='s3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url,
        )

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        exc_traceback
    ):
        self.client.close()

    def close_client_session(self) -> None:
        self.client.close()

    def put_json_object(
        self,
        data: dict,
        bucket_name: str,
        file_path: str,
    ) -> int:
        resp = self.client.put_object(
            Body=json.dumps(data),
            Bucket=bucket_name,
            Key=file_path,
        )

        return resp.get(
            'ResponseMetadata'
        ).get(
            'HTTPStatusCode'
        )
