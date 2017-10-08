#!/usr/bin/env python
"""
Usage:

python redshift-unload-copy.py <config file> <region>


* Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Amazon Software License (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* http://aws.amazon.com/asl/
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
"""

import sys
import pg
import json
import base64
import boto3
import re
import logging
import datetime

kmsClient = None
nowString = None
config = None
region = None
bucket = None
key = None

encryptionKeyID = 'alias/RedshiftUnloadCopyUtility'

options = """keepalives=1 keepalives_idle=200 keepalives_interval=200 
             keepalives_count=6"""

set_timeout_stmt = "set statement_timeout = 1200000"

unload_stmt = """unload ('SELECT * FROM %s.%s')
                 to '%s' credentials 
                 '%s;master_symmetric_key=%s'
                 manifest
                 encrypted
                 gzip
                 delimiter '^' addquotes escape allowoverwrite"""

copy_stmt = """copy %s.%s
               from '%smanifest' credentials 
               '%s;master_symmetric_key=%s'
               manifest 
               encrypted
               gzip
               delimiter '^' removequotes escape"""


def conn_to_rs(host, port, db, usr, pwd, opt=options, timeout=set_timeout_stmt):
    rs_conn_string = """host=%s port=%s dbname=%s user=%s password=%s 
                         %s""" % (host, port, db, usr, pwd, opt)
    print("Connecting to %s:%s:%s as %s" % (host, port, db, usr))
    rs_conn = pg.connect(dbname=rs_conn_string)
    rs_conn.query(timeout)
    return rs_conn


def unload_data(conn, s3_access_credentials, master_symmetric_key, dataStagingPath, schema_name, table_name):
    print("Exporting %s.%s to %s" % (schema_name, table_name, dataStagingPath))
    conn.query(unload_stmt % (schema_name, table_name, dataStagingPath, s3_access_credentials, master_symmetric_key))


def copy_data(conn, s3_access_credentials, master_symmetric_key, dataStagingPath, dataStagingRegion, schema_name,
              table_name):
    global copy_stmt
    if dataStagingRegion != None:
        copy_stmt = copy_stmt + ("\nREGION '%s'" % (dataStagingRegion))

    print("Importing %s.%s from %s" % (
    schema_name, table_name, dataStagingPath + (":%s" % (dataStagingRegion) if dataStagingRegion != None else "")))
    conn.query(copy_stmt % (schema_name, table_name, dataStagingPath, s3_access_credentials, master_symmetric_key))


def decrypt(b64EncodedValue):
    return kmsClient.decrypt(CiphertextBlob=base64.b64decode(b64EncodedValue))['Plaintext']


def tokeniseS3Path(path):
    pathElements = path.split('/')
    bucketName = pathElements[2]
    prefix = "/".join(pathElements[3:])

    return (bucketName, prefix)


def getConfig(path):
    """
    This is only in place to allow regression testing to show that AWS interactions are still working
    :param path:
    :return:
    """
    # datetime alias for operations
    global config

    if path.startswith("s3://"):
        # download the configuration from s3
        s3_client = S3Client(region)

        config = s3_client.get_json_config_as_dict(path)
    else:
        with open(path) as f:
            config = json.load(f)


def get_cluster_endpoint_regex():
    """
    A cluster endpoint is comprised of letters, digits, or hyphens

    From http://docs.aws.amazon.com/redshift/latest/mgmt/managing-clusters-console.html
        They must contain from 1 to 63 alphanumeric characters or hyphens.
        Alphabetic characters must be lowercase.
        The first character must be a letter.
        They cannot end with a hyphen or contain two consecutive hyphens.
        They must be unique for all clusters within an AWS account.
    :return:
    """
    cluster_endpoint_regex_parts = [
        {
            'name': 'cluster_identifier',
            'pattern': '[a-z][a-z0-9-]*'
        },
        {
            'pattern': r'\.'
        },
        {
            'name': 'customer_hash',
            'pattern': r'[0-9a-z]+'
        },
        {
            'pattern': r'\.'
        },
        {
            'name': 'region',
            'pattern': '[a-z]+-[a-z]+-[0-9]+'
        },
        {
            'pattern': r'\.redshift\.amazonaws\.com$'
        }
    ]
    pattern = ''
    for element in cluster_endpoint_regex_parts:
        if 'name' in element.keys():
            pattern += '(?P<' + element['name'] + '>'
        pattern += element['pattern']
        if 'name' in element.keys():
            pattern += ')'
    return re.compile(pattern)


def get_element_from_cluster_endpoint(clusterEndpoint, element):
    match_result = get_cluster_endpoint_regex().match(clusterEndpoint.lower())
    if match_result is not None:
        return match_result.groupdict()[element]
    else:
        logging.fatal('Could not extract region from cluster endpoint {ce}'.format(ce=clusterEndpoint))


def get_region_from_cluster_endpoint(clusterEndpoint):
    return get_element_from_cluster_endpoint(clusterEndpoint, 'region')


def get_cluster_identifier_from_cluster_endpoint(clusterEndpoint):
    return get_element_from_cluster_endpoint(clusterEndpoint, 'cluster_identifier')


def getClusterPassword(clusterEndpoint, userName, autoCreate=False, dbGroups=None):
    logging.debug("Try getting DB credentials for {u}@{c}".format(u=userName, c=clusterEndpoint))
    redshiftClient = boto3.client('redshift', region_name=get_region_from_cluster_endpoint(clusterEndpoint))


def usage():
    print("Redshift Unload/Copy Utility")
    print(
    "Exports data from a source Redshift database to S3 as an encrypted dataset, and then imports into another Redshift Database")
    print("")
    print("Usage:")
    print("python redshift_unload_copy.py <configuration> <region>")
    print("    <configuration> Local Path or S3 Path to Configuration File on S3")
    print(
    "    <region> Region where Configuration File is stored (S3) and where Master Keys and Data Exports are stored")
    sys.exit(-1)


class S3Client:
    def __init__(self, region_name):
        self.s3_client = boto3.client('s3', region_name=region)

    def get_json_config_as_dict(self, s3_url):
        # datetime alias for operations
        global nowString
        if nowString is None:
            nowString = "{:%Y-%m-%d_%H-%M-%S}".format(datetime.datetime.now())

        if s3_url.startswith("s3://"):
            # download the configuration from s3
            (config_bucket_name, config_key) = tokeniseS3Path(s3_url)

            response = self.s3_client.get_object(Bucket=config_bucket_name,
                                           Key=config_key)  # Throws NoSuchKey exception if no config
            configContents = response['Body'].read(1024 * 1024).decode('utf-8')  # Read maximum 1MB

            config = json.loads(configContents)
        else:
            with open(s3_url) as f:
                config = json.load(f)
        return config

    def delete_list_of_keys_from_bucket(self, keys_to_delete, bucket_name):
        """
        This is a wrapper around delete_objects for the boto3 S3 client.
        This call only allows a maximum of 1000 keys otherwise an Exception will be thrown
        :param keys_to_delete:
        :param bucket_name:
        :return:
        """
        if len(keys_to_delete) > 1000:
            raise Exception('Batch delete only supports a maximum of 1000 keys at a time')

        object_list = []
        for key in keys_to_delete:
            object_list.append({'Key': key})
        self.s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': object_list})

    def delete_s3_prefix(self, staging_path):
        print("Cleaning up S3 Data Staging Location %s" % staging_path)
        (stagingBucket, stagingPrefix) = tokeniseS3Path(staging_path)

        objects = self.s3_client.list_objects_v2(Bucket=stagingBucket, Prefix=stagingPrefix)
        if objects['KeyCount'] > 0:
            keys_to_delete = []
            key_number = 1
            for s3_object in objects['Contents']:
                if (key_number % 1000) == 0:
                    self.delete_list_of_keys_from_bucket(keys_to_delete, stagingBucket)
                    keys_to_delete = []
                keys_to_delete.append(s3_object['Key'])
            self.delete_list_of_keys_from_bucket(keys_to_delete, stagingBucket)


def main(args):
    if len(args) != 2:
        usage

    global region
    region = args[2]

    s3Client = S3Client(region)

    global kmsClient
    kmsClient = boto3.client('kms', region_name=region)

    # load the configuration
    global config
    config = s3Client.get_json_config_as_dict(args[1])

    # parse options
    dataStagingPath = "%s/%s/" % (config['s3Staging']['path'].rstrip("/"), nowString)
    if not dataStagingPath.startswith("s3://"):
        print("s3Staging.path must be a path to S3")
        sys.exit(-1)

    dataStagingRegion = None
    if 'region' in config["s3Staging"]:
        dataStagingRegion = config["s3Staging"]['region']

    s3_access_credentials = ''
    if 'aws_iam_role' in config["s3Staging"]:
        accessRole = config['s3Staging']['aws_iam_role']
        s3_access_credentials = "aws_iam_role=%s" % accessRole
    else:
        accessKey = config['s3Staging']['aws_access_key_id']
        secretKey = config['s3Staging']['aws_secret_access_key']

        # decrypt aws access keys
        s3_access_key = decrypt(accessKey)
        s3_secret_key = decrypt(secretKey)

        s3_access_credentials = "aws_access_key_id=%s;aws_secret_access_key=%s" % (s3_access_key, s3_secret_key)

    deleteOnSuccess = config['s3Staging']['deleteOnSuccess']

    # source from which to export data
    srcConfig = config['unloadSource']

    src_host = srcConfig['clusterEndpoint']
    src_port = srcConfig['clusterPort']
    src_db = srcConfig['db']
    src_schema = srcConfig['schemaName']
    src_table = srcConfig['tableName']
    src_user = srcConfig['connectUser']

    # target to which we'll import data
    destConfig = config['copyTarget']

    dest_host = destConfig['clusterEndpoint']
    dest_port = destConfig['clusterPort']
    dest_db = destConfig['db']
    dest_schema = destConfig['schemaName']
    dest_table = destConfig['tableName']
    dest_user = destConfig['connectUser']

    # create a new data key for the unload operation
    dataKey = kmsClient.generate_data_key(KeyId=encryptionKeyID, KeySpec="AES_256")

    master_symmetric_key = base64.b64encode(dataKey['Plaintext'])

    # decrypt the source and destination passwords
    src_pwd = decrypt(srcConfig["connectPwd"])
    dest_pwd = decrypt(destConfig["connectPwd"])

    print("Exporting from Source")
    src_conn = conn_to_rs(src_host, src_port, src_db, src_user,
                          src_pwd)
    unload_data(src_conn, s3_access_credentials, master_symmetric_key, dataStagingPath,
                src_schema, src_table)

    print("Importing to Target")
    dest_conn = conn_to_rs(dest_host, dest_port, dest_db, dest_user,
                           dest_pwd)
    copy_data(dest_conn, s3_access_credentials, master_symmetric_key, dataStagingPath, dataStagingRegion,
              dest_schema, dest_table)

    src_conn.close()
    dest_conn.close()

    if 'true' == deleteOnSuccess.lower():
        s3Client.delete_s3_prefix(dataStagingPath)


if __name__ == "__main__":
    main(sys.argv)
