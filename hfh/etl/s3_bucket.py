import boto3
import botocore
import mgmt

cfg = mgmt.cfg
logger = mgmt.logger
S3_KEY = cfg.get('S3_BUCKET', 'S3_KEY')
S3_SECRET = cfg.get('S3_BUCKET', 'S3_SECRET')
S3_BUCKET_NAME = cfg.get('S3_BUCKET', 'S3_BUCKET_NAME')

s3 = boto3.resource(
            's3',
            aws_access_key_id=S3_KEY,
            aws_secret_access_key=S3_SECRET,
    )


bucket = s3.Bucket(S3_BUCKET_NAME)


def upload(file_from, file_to,
           contenttype='image',
           acl='public-read'):
    try:
        data = open(file_from, 'rb')
        bucket.put_object(Key=file_to,
                          Body=data,
                          ContentType=contenttype,
                          ACL=acl)
        logger.info('file: '+file_from+'\t upload to: '+file_to)
    except botocore.exceptions.ClientError:
        logger.warn('Uploaded Failed: '+file_from+'\t to: '+file_to)
