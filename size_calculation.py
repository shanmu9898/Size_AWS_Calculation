import datetime
import boto3
import pytz
import re
import argparse

def get_size(bucket, prefix, date, regex):
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    # Convert date string to datetime object
    start_date = None
    if date:
        date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')

        # Make start and end dates timezone-aware
        timezone = pytz.timezone('UTC')
        start_date = timezone.localize(datetime.datetime.combine(date_obj, datetime.time.min)).isoformat()

    paginator = s3.get_paginator('list_objects')
    if prefix:
        objects = paginator.paginate(Bucket=bucket, Prefix=prefix)
    else:
        objects = paginator.paginate(Bucket=bucket)


    # Calculate the total size of all objects
    total_size = 0
    count = 0
    for obj in objects:
        for content in obj.get('Contents', []):
            last_modified = content.get('LastModified')
            if start_date and not str(last_modified).startswith(date):
                continue
            if regex and not re.search(regex, content.get('Key')):
                continue
            total_size += content.get('Size')
            count += 1

    return total_size, count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="my-bucket-name", help="S3 bucket name")
    parser.add_argument("--folder", help="Folder name inside S3 bucket")
    parser.add_argument("--date", help="Last modified date of the files in format 'YYYY-MM-DD'")
    parser.add_argument("--regex", default="", help="Regex to filter for specific files")
    args = parser.parse_args()

    regex_string = f".*{args.regex}.*"
    regex = re.compile(regex_string)

    total_size, count = get_size(args.bucket, args.folder, args.date, regex)
    print(f'Total file size: {total_size / (1024*1024*1024):.6f} GB in {count} files')

if __name__ == '__main__':
    main()
