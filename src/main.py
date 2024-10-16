from os import environ

from .listener.listener import Listener

if __name__ == '__main__':
    listener = Listener(
        queue_name=environ["SQS_QUEUE_NAME"],
        table_name=environ["DYNAMODB_TABLE_NAME"],
        output_bucket_name=environ["OUTPUT_S3_BUCKET_NAME"],
        region=environ["AWS_REGION"]
    )
    listener.start()
    listener.join()
