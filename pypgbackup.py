import argparse
import subprocess
import datetime
import os
import getpass
import tempfile
import sys

def get_dt_format():
    """
    Gets the datetime format string.

    Returns:
        str:  The datetime format string.
    """
    return '%Y%m%d-%H%M%S'

def create_arg_parser():
    """
    Creates an argument parser.

    Returns:
        The argparser.
    """
    parser = argparse.ArgumentParser(description='Make postgres backups to cloud locations.')
    parser.add_argument('-H', '--hostname', default='localhost', help='The hostname of the postgres server.')
    parser.add_argument('-p', '--port', type=int, default=5432, help='The port of the postgres server.')
    parser.add_argument('-U', '--user', help='The postgres user.', required=True)
    parser.add_argument('-d', '--database', help='The postgres database to back up.', required=True)
    parser.add_argument('-b2', '--b2-bucket', help='The b2 bucket to upload to.')
    parser.add_argument('--b2-prefix', default='', help='The b2 bucket to upload to.')    
    return parser


def get_password():
    """
    Gets a password either from the PGPASSWORD environment variable
    or from input.

    Returns:
        str:  The password.
    """
    if 'PGPASSWORD' in os.environ:
        return os.environ['PGPASSWORD']

    else:
        return getpass.getpass(prompt="password: ")
    

def create_backup(host, port, user, password, database, directory):
    """
    Creates the PostgreSQL backup.

    Args:
        host (str):  The hostname of the postgres server.
        port (int):  The port number of the postgres server.
        user (str):  The postgres username.
        password (str):  The postgres password.
        database (str):  The postgres database to backup.
        directory (str):  The firectory to save the file in.

    Returns:
        str:  The path to the database export file.
    """
    if 'PGPASSWORD' not in os.environ:
        os.environ['PGPASSWORD'] = password

    filename = '{}_{}.backup'.format(
        database, datetime.datetime.utcnow().strftime(get_dt_format())
        )
    filepath = os.path.join(directory, filename)

    cmd = ['pg_dump', '-h', host,
           '-p', str(port),
           '-U', user,
           '-f', filepath,
           '-Fc', '-Z9',
           '-d', database]

    completed_process = subprocess.run(' '.join(cmd), shell=True, stdout=subprocess.PIPE, stderr=sys.stdout)
    assert completed_process.returncode == 0, completed_process.stdout
    
    return filepath


def upload_to_b2_bucket(filepath, bucket_name, prefix):
    """
    Upload backup to bucket.

    Args:
        filepath (str):  The path of the file to upload.
        bucket_name (str):  The name of the bucket to upload to.
    """
    from b2blaze import B2

    # get the keys
    assert 'B2_KEY_ID' in os.environ
    assert 'B2_APPLICATION_KEY' in os.environ

    b2 = B2()

    bucket_names = list(map(lambda x: x.bucket_name, b2.buckets.all()))
    assert bucket_name in bucket_names, \
        'Bucket {} not in {}'.format(bucket_name, bucket_names)
    bucket = b2.buckets.get(bucket_name)

    if len(prefix.strip()) > 0 and prefix[:-1] != '/':
        prefix = prefix.strip() + '/'
        
    with open(filepath, 'rb') as f:
        bucket.files.upload(contents=f,
                            file_name=os.path.basename(filepath))

def main():
    parser = create_arg_parser()
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tf:
        filepath = create_backup(args.hostname,
                                 args.port,
                                 args.user,
                                 get_password(),
                                 args.database,
                                 tf)

        if args.b2_bucket is not None:
            upload_to_b2_bucket(filepath, args.b2_bucket, args.b2_prefix)


if __name__ == '__main__':
    main()
