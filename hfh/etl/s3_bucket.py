from __future__ import division
import boto3
import botocore
import mgmt
from PIL import Image
from io import BytesIO

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
        data = open(file_from, 'rb')\
                if not isinstance(file_from, BytesIO) else file_from
        file_from = file_from if not isinstance(file_from, BytesIO) else 'Img'
        bucket.put_object(Key=file_to,
                          Body=data,
                          ContentType=contenttype,
                          ACL=acl)
        logger.info('file: '+file_from+'\t upload to: '+file_to)
    except botocore.exceptions.ClientError as e:
        logger.error(e)
        logger.error('Uploaded Failed: '+file_from+'\t to: '+file_to)


def resize(im, max_height=128):
    # Original Size
    w, h = im.size

    # New width with original ratio
    w = int((w/h)*max_height)
    h = max_height

    # New size with given max height and original ratio
    new_size = (w, h)
    print(new_size)

    return im.resize(new_size)


def upload_image(image, upload_key):
    im = Image.open(image)

    out_im = BytesIO()
    out_im_thumbnail = BytesIO()

    # Original Image Uploaded
    im.save(out_im, 'jpeg', optimize=True, progressive=True)

    # thumbnail_size = (128, 128)
    # im.thumbnail(thumbnail_size, Image.ANTIALIAS)
    im_th = resize(im)

    # Resized thumbnail Image Uploaded
    # im.save(out_im_thumbnail, 'jpeg', progressive=True)
    im_th.save(out_im_thumbnail, 'jpeg', progressive=True)

    # file pointer to beginning
    out_im.seek(0)
    out_im_thumbnail.seek(0)
    # Upload Thumbnail
    upload(out_im_thumbnail, 'thumbnail/'+upload_key)
    # Upload Original
    upload(out_im, upload_key)
