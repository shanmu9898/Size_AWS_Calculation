import datetime
import boto3
import pytz

def get_size(bucket, prefix, date):
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    # Convert date string to datetime object
    date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')

    # Make start and end dates timezone-aware
    timezone = pytz.timezone('UTC')
    start_date = timezone.localize(datetime.datetime.combine(date_obj, datetime.time.min)).isoformat()
    end_date = timezone.localize(datetime.datetime.combine(date_obj, datetime.time.max)).isoformat()

    # Get all objects in the specified prefix and date range
    objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1000, StartAfter=start_date)

    # Calculate the total size of all objects
    total_size = 0
    for obj in objects['Contents']:
        if obj['LastModified'].strftime('%Y-%m-%d') == date:
            total_size += obj['Size']

    return total_size


def main():
    bucket_name = 'mktestvimo'
    folder_name = 'Test/20170227195450'
    date = '2017-02-27'
    total_size = get_size(bucket_name, folder_name, date)

    print(f'Total file size: {total_size} bytes')

if __name__ == '__main__':
    main()
