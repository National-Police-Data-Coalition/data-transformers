from datetime import datetime, UTC
from io import BytesIO, StringIO
from logging import getLogger
from threading import Thread
from time import sleep

from boto3 import Session
import ujson

from ..transformers.transformer import Transformer

class Listener(Thread):
    def __init__(self, queue_name: str, table_name: str, output_bucket_name: str, region: str = "us-east-1"):
        # TODO: ideally we would have a function on the app to catch shutdown
        #  events and close gracefully, but until then daemon it is.
        super().__init__(daemon=True)
        self.queue_name = queue_name
        self.session = Session(region_name=region)
        self.sqs_client = self.session.client("sqs")
        self.s3_client = self.session.client("s3")
        self.dynamodb_client = self.session.client("dynamodb")
        self.sqs_queue_url = self.sqs_client.get_queue_url(
            QueueName=self.queue_name)
        self.dynamodb_table_name = table_name
        self.output_bucket_name = output_bucket_name
        self.logger = getLogger(self.__class__.__name__)

        self.transformer_map: dict[str, Transformer] = {}

    def run(self):
        while True:
            try:
                resp = self.sqs_client.receive_message(
                    QueueUrl=self.sqs_queue_url,
                    # retrieve one message at a time - we could up this
                    # and parallelize but no point until way more files.
                    MaxNumberOfMessages=1,
                    # 10 minutes to process message before it becomes
                    # visible for another consumer.
                    VisibilityTimeout=600,
                )
                # if no messages found, wait 5m for next poll
                if len(resp["Messages"]) == 0:
                    sleep(600)
                    continue

                for message in resp["Messages"]:
                    sqs_body = ujson.loads(message["Body"])
                    # this comes through as a list, but we expect one object
                    for record in sqs_body["Records"]:
                        bucket_name = record["s3"]["bucket"]["name"]
                        key = record["s3"]["object"]["key"]

                        # TODO: check dynamodb table to reduce data duplication

                        with BytesIO() as fileobj:
                            self.s3_client.download_fileobj(
                                bucket_name, key, fileobj)
                            fileobj.seek(0)
                            content = fileobj.read()
                            _ = content  # for linting.

                        # TODO: we now have an in-memory copy of s3 file content
                        #  This is where we would run the importer.
                        #  we want a standardized importer class; use like:
                        #   transformer = self.get_transformer_for_content_type(key)
                        #   transformed_content = transformer(content).transform()

                        self.logger.info(f"Transformed s3://{bucket_name}/{key}")

                        transformed_content = list({"test": "converted"})  # TODO: temporary until we have a converter

                        output_key = self.generate_target_s3_path(key)
                        with StringIO() as fileobj:
                            fileobj.write("\n".join(ujson.dumps(content) for content in transformed_content))
                            fileobj.seek(0)
                            self.s3_client.upload_fileobj(
                                fileobj, self.output_bucket_name, output_key)

                        self.logger.info(f"Uploaded transformed content to s3://{self.output_bucket_name}/{output_key}")

                        # TODO: update dynamodb cache to store processed files to reduce duplication.
            except Exception as e:
                self.logger.error(
                    f"Failed to process scraper events sqs queue: {e}")
                sleep(600)
                pass

    def get_transformer_for_content_type(self, s3_key: str) -> Transformer:
        # s3 keys should be of format /subject/source/time.jsonl
        prefix = "/".join(s3_key.split("/")[:-1])
        return self.transformer_map[prefix]

    @staticmethod
    def generate_target_s3_path(input_s3_key: str) -> str:
        components = input_s3_key.split("/")[:-1]
        components.append(datetime.now(tz=UTC).isoformat())
        return "/".join(components)
