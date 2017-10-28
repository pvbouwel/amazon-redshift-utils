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

region = None

encryptionKeyID = 'alias/RedshiftUnloadCopyUtility'

options = """keepalives=1 keepalives_idle=200 keepalives_interval=200
             keepalives_count=6"""

set_timeout_stmt = "set statement_timeout = 1200000"


def usage():
    print("Redshift Unload/Copy Utility")
    print("Exports data from a source Redshift database to S3 as an encrypted dataset, "
          "and then imports into another Redshift Database")
    print("")
    print("Usage:")
    print("python redshift_unload_copy.py <configuration> <region>")
    print("    <configuration> Local Path or S3 Path to Configuration File on S3")
    print("    <region> Region where Configuration File is stored (S3) "
          "and where Master Keys and Data Exports are stored")
    sys.exit(-1)


class KMSHelper:
    def __init__(self, region_name):
        self.kms_client = boto3.client('kms', region_name=region_name)

    def generate_base64_encoded_data_key(self, encryption_key_id, key_spec="AES_256"):
        data_key = self.kms_client.generate_data_key(KeyId=encryption_key_id, KeySpec=key_spec)
        return base64.b64encode(data_key['Plaintext']).decode('utf-8')

    def decrypt(self, b64_encoded_value):
        return self.kms_client.decrypt(CiphertextBlob=base64.b64decode(b64_encoded_value))['Plaintext']

    @staticmethod
    def generate_data_key_without_kms():
        if (sys.version_info[0] == 3 and sys.version_info[1] >= 6) or sys.version_info[0] > 3:
            # Use new secrets module https://docs.python.org/3/library/secrets.html
            import secrets
            return secrets.token_bytes(int(256 / 8))
        else:
            # Legacy code to generate random value
            try:
                from Crypto import Random
            except ImportError:
                pycrypto_explanation = """
                For generating a secure Random sequence without KMS, pycrypto is used in case you use Python <3.6 .
                This does not seem to be available on your system and therefore execution needs to be aborted.
                In order to not use KMS in case of a Python to setup you must install the pycrypto library.
                For example using `pip install pycrypto`

                It requires to compile code in C so there are some requirements you need to satisfy. For more info
                check out the installation section in the source documentation at https://pypi.python.org/pypi/pycrypto

                Alternatively you could use KMS by setting s3Staging -> kmsGeneratedKey to True in the config file
                In that case make sure to generate a key using ./createKmsKey.sh <region-short-name>
                """
                logging.fatal(pycrypto_explanation)
                sys.exit(-5)
            return Random.new().read(256 / 8)


class TableResourceFactory:
    def __init__(self):
        pass

    @staticmethod
    def get_source_table_resource_from_config_helper(config_helper, kms_region=None):
        cluster_dict = config_helper.config['unloadSource']
        return TableResourceFactory.get_table_resource_from_dict(cluster_dict, kms_region)

    @staticmethod
    def get_target_table_resource_from_config_helper(config_helper, kms_region=None):
        cluster_dict = config_helper.config['copyTarget']
        return TableResourceFactory.get_table_resource_from_dict(cluster_dict, kms_region)

    @staticmethod
    def get_cluster_from_cluster_dict(cluster_dict, kms_region):
        cluster = RedshiftCluster(cluster_dict['clusterEndpoint'])
        cluster.set_port(cluster_dict['clusterPort'])
        cluster.set_user(cluster_dict['connectUser'])
        cluster.set_host(cluster_dict['clusterEndpoint'])
        cluster.set_db(cluster_dict['db'])
        if 'connectPwd' in cluster_dict:
            if kms_region is None:
                kms_region = cluster.get_region_name()
            kms_helper = KMSHelper(kms_region)
            cluster.set_password(kms_helper.decrypt(cluster_dict['connectPwd']))

        cluster.set_user_auto_create(False)
        if 'userAutoCreate' in cluster_dict \
                and cluster_dict['userAutoCreate'].lower() == 'true':
            cluster.set_user_auto_create(True)

        cluster.user_db_groups = []
        if 'userDbGroups' in cluster_dict:
            cluster.set_user_db_groups(cluster_dict['userDbGroups'])
        return cluster

    @staticmethod
    def get_table_resource_from_dict(cluster_dict, kms_region):
        cluster = TableResourceFactory.get_cluster_from_cluster_dict(cluster_dict, kms_region)
        table_resource = TableResource(cluster, cluster_dict['schemaName'], cluster_dict['tableName'])
        return table_resource


class TableResource:
    commands = {}
    unload_stmt = """unload ('SELECT * FROM {schema_name}.{table_name}')
                     to '{dataStagingPath}{schema_name}.{table_name}.' credentials 
                     '{s3_access_credentials};master_symmetric_key={master_symmetric_key}'
                     manifest
                     encrypted
                     gzip
                     delimiter '^' addquotes escape allowoverwrite"""
    commands['unload'] = unload_stmt

    copy_stmt = """copy {schema_name}.{table_name}
                   from '{dataStagingPath}{schema_name}.{table_name}.manifest' credentials 
                   '{s3_access_credentials};master_symmetric_key={master_symmetric_key}'
                   manifest 
                   encrypted
                   gzip
                   delimiter '^' removequotes escape compupdate off"""
    commands['copy'] = copy_stmt

    def get_schema(self):
        return self._schema

    def set_schema(self, schema):
        self._schema = schema

    def get_table(self):
        return self._table

    def set_table(self, table):
        self._table = table

    def get_cluster(self):
        return self._cluster

    def set_cluster(self, cluster):
        self._cluster = cluster

    def __init__(self, rs_cluster, schema, table):
        self._cluster = rs_cluster
        self._schema = schema
        self._table = table

    def run_command_against_table_resource(self, command, command_parameters):
        command_parameters['schema_name'] = self.get_schema()
        command_parameters['table_name'] = self.get_table()
        command_parameters['cluster'] = self.get_cluster()
        logging.info("Executing on {cluster} the command: {command}")
        command_to_execute = self.commands[command]
        if 'region' in command_parameters and command == 'copy' and command_parameters['region'] is not None:
            command_to_execute += " REGION '{region}' "
        self.get_cluster().execute_query(command_to_execute.format(**command_parameters))

    def unload_data(self, s3_details):
        unload_parameters = {'s3_access_credentials': s3_details.access_credentials,
                             'master_symmetric_key': s3_details.symmetric_key,
                             'dataStagingPath': s3_details.dataStagingPath,
                             'region': s3_details.dataStagingRegion}
        self.run_command_against_table_resource('unload', unload_parameters)

    def copy_data(self, s3_details):
        copy_parameters = {'s3_access_credentials': s3_details.access_credentials,
                           'master_symmetric_key': s3_details.symmetric_key,
                           'dataStagingPath': s3_details.dataStagingPath,
                           'region': s3_details.dataStagingRegion}

        self.run_command_against_table_resource('copy', copy_parameters)


class RedshiftCluster:
    def __init__(self, cluster_endpoint):
        self._password = None
        self._user = None
        self._db = None
        self._host = None
        self._port = None
        self.cluster_endpoint = cluster_endpoint
        self._user_auto_create = False
        self._user_creds_expiration = datetime.datetime.now()
        self._user_db_groups = []

    def get_user(self):
        return self._user

    def set_user(self, user):
        self._user = user

    def get_password(self):
        if self._password is None or self.is_temporary_credential_expired():
            self.refresh_temporary_credentials()
        return self._password

    def set_password(self, password):
        self._password = password

    def get_host(self):
        return self._host

    def set_host(self, host):
        self._host = host


    def get_port(self):
        return self._port

    def set_port(self, port):
        self._port = port

    def get_db(self):
        return self._db

    def set_db(self, db):
        self._db = db

    def get_user_auto_create(self):
        return self._user_auto_create

    def set_user_auto_create(self, user_auto_create):
        self._user_auto_create = user_auto_create


    def get_user_db_groups(self):
        return self._user_db_groups

    def set_user_db_groups(self, user_db_groups):
        self._user_db_groups = user_db_groups

    def get_user_creds_expiration(self):
        return self._user_creds_expiration

    def set_user_creds_expiration(self, user_creds_expiration):
        self._user_creds_expiration = user_creds_expiration


    def is_temporary_credential_expired(self):
        one_minute_from_now = datetime.datetime.now() + datetime.timedelta(minutes=1)
        if self.get_user_creds_expiration() is None:
            return True

        if one_minute_from_now > self.get_user_creds_expiration():
            return True
        return False

    def refresh_temporary_credentials(self):
        logging.debug("Try getting DB credentials for {u}@{c}".format(u=self.get_user(), c=self.get_host()))
        redshift_client = boto3.client('redshift', region_name=self.get_region_name())
        get_creds_params = {
            'DbUser': self.get_user(),
            'DbName': self.get_db(),
            'ClusterIdentifier': self.get_cluster_identifier()
        }
        if self.get_user_auto_create():
            get_creds_params['AutoCreate'] = True
        if len(self.get_user_db_groups()) > 0:
            get_creds_params['DbGroups'] = self.get_user_db_groups()
        response = redshift_client.get_cluster_credentials(**get_creds_params)
        self.set_user(response['DbUser'])
        self.set_password(response['DbPassword'])
        self.set_user_creds_expiration(response['Expiration'])

    @staticmethod
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

    def get_element_from_cluster_endpoint(self, element):
        match_result = RedshiftCluster.get_cluster_endpoint_regex().match(self.cluster_endpoint.lower())
        if match_result is not None:
            return match_result.groupdict()[element]
        else:
            logging.fatal('Could not extract region from cluster endpoint {cluster_endpoint}'.format(
                cluster_endpoint=self.cluster_endpoint.lower()))

    def get_region_name(self):
        return self.get_element_from_cluster_endpoint('region')

    def get_cluster_identifier(self):
        return self.get_element_from_cluster_endpoint('cluster_identifier')

    def _conn_to_rs(self, opt=options, timeout=set_timeout_stmt):
        rs_conn_string = "host={host} port={port} dbname={db} user={user} password={password} {opt}".format(
            host=self.get_host(),
            port=self.get_port(),
            db=self.get_db(),
            password=self.get_password(), # Very important to first fetch the password because temporary password updates user!
            user=self.get_user(),
            opt=opt)
        print("Connecting to {host}:{port}:{db} as {user}".format(host=self.get_host(),
                                                                  port=self.get_port(),
                                                                  db=self.get_db(),
                                                                  user=self.get_user()))
        rs_conn = pg.connect(dbname=rs_conn_string)
        self._conn = rs_conn

    def execute_query(self, command, opt=options, timeout=set_timeout_stmt):
        self._conn_to_rs(opt=options, timeout=set_timeout_stmt)
        self._conn.query(timeout)
        print(command)
        self._conn.query(command)
        self._disconnect_from_rs()

    def _disconnect_from_rs(self):
        self._conn.close()


class S3Helper:
    def __init__(self, region_name):
        self.region_name = region_name
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.config = None

    def get_json_config_as_dict(self, s3_url):
        if s3_url.startswith("s3://"):
            # download the configuration from s3
            (config_bucket_name, config_key) = S3Helper.tokenise_S3_Path(s3_url)

            response = self.s3_client.get_object(Bucket=config_bucket_name,
                                           Key=config_key)  # Throws NoSuchKey exception if no config
            config_contents = response['Body'].read(1024 * 1024).decode('utf-8')  # Read maximum 1MB

            config = json.loads(config_contents)
        else:
            with open(s3_url) as f:
                config = json.load(f)

        self.config = config
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

    def delete_s3_prefix(self, s3_details):
        print("Cleaning up S3 Data Staging Location %s" % s3_details.dataStagingPath)
        (stagingBucket, stagingPrefix) = S3Helper.tokenise_S3_Path(s3_details.dataStagingPath)

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

    @staticmethod
    def tokenise_S3_Path(path):
        path_elements = path.split('/')
        bucket_name = path_elements[2]
        prefix = "/".join(path_elements[3:])

        return bucket_name, prefix


class ConfigHelper:
    def __init__(self, config_path, s3_helper=None):
        self.s3_helper = s3_helper

        if config_path.startswith("s3://"):
            if s3_helper is None:
                raise Exception('No region set to get config file but it resides on S3')
            self.config = s3_helper.get_json_config_as_dict(config_path)
        else:
            with open(config_path) as f:
                self.config = json.load(f)


class S3AccessCredentialsKey:
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def __str__(self):
        return 'aws_access_key_id={key};aws_secret_access_key={secret}'.format(
            key=self.aws_access_key_id, secret=self.aws_secret_access_key
        )


class S3AccessCredentialsRole:
    def __init__(self, aws_iam_role):
        self.aws_iam_role = aws_iam_role

    def __str__(self):
        return 'aws_iam_role={role}'.format(role=self.aws_iam_role)


class S3Details:
    class NoS3CredentialsFoundException(Exception):
        def __init__(self, *args):
            super().__init__(*args)

    class NoS3StagingInformationFoundException(Exception):
        def __init__(self, *args):
            super().__init__(*args)

    class S3StagingPathMustStartWithS3(Exception):
        def __init__(self, *args):
            super().__init__('s3Staging.path must be a path to S3, so start with s3://', *args)

    def __init__(self, config_helper):
        if 's3Staging' not in config_helper.config:
            raise S3Details.NoS3StagingInformationFoundException()
        else:
            s3_staging_conf = config_helper.config['s3Staging']
            if 'region' in s3_staging_conf:
                self.dataStagingRegion = s3_staging_conf['region']
            else:
                logging.warning('No region in s3_staging_conf')
                self.dataStagingRegion = None

            if 'deleteOnSuccess' in s3_staging_conf \
                    and s3_staging_conf['deleteOnSuccess'].lower() == 'true':
                self.deleteOnSuccess = True
            else:
                self.deleteOnSuccess = False

            if 'path' in s3_staging_conf:
                # datetime alias for operations
                self.nowString = "{:%Y-%m-%d_%H-%M-%S}".format(datetime.datetime.now())
                self.dataStagingPath = "%s/%s/" % (s3_staging_conf['path'].rstrip("/"), self.nowString)

            if not self.dataStagingPath or not self.dataStagingPath.startswith("s3://"):
                raise S3Details.S3StagingPathMustStartWithS3

            if 'aws_iam_role' in s3_staging_conf:
                role = s3_staging_conf['aws_iam_role']
                self.access_credentials = S3AccessCredentialsRole(role)
            elif 'aws_access_key_id' in s3_staging_conf and 'aws_secret_access_key' in s3_staging_conf:
                kms_helper = KMSHelper(config_helper.region_name)
                key_id = kms_helper.decrypt(s3_staging_conf['aws_access_key_id'])
                secret_key = kms_helper.decrypt(s3_staging_conf['aws_secret_access_key'])
                self.access_credentials = S3AccessCredentialsKey(key_id, secret_key)
            else:
                raise(S3Details.NoS3CredentialsFoundException())

            use_kms = True
            if 'kmsGeneratedKey' in s3_staging_conf:
                if s3_staging_conf['kmsGeneratedKey'].lower() == 'false':
                    use_kms = False

            if use_kms:
                kms_helper = KMSHelper(config_helper.s3_helper.region_name)
                self.symmetric_key = kms_helper.generate_base64_encoded_data_key(encryptionKeyID)
            else:
                self.symmetric_key = base64.b64encode(KMSHelper.generate_data_key_without_kms())
            try:
                self.symmetric_key = self.symmetric_key.decode('utf-8')
            except:
                pass # if fails then already string type probably Python2

class UnloadCopyTool:
    def __init__(self, config_file, region):
        self.region = region
        self.s3_helper = S3Helper(self.region)

        # load the configuration
        self.config_helper = ConfigHelper(config_file, self.s3_helper)

        self.s3_details = S3Details(self.config_helper)

        source_table = TableResourceFactory.get_source_table_resource_from_config_helper(self.config_helper,
                                                                                         self.region)
        destination_table = TableResourceFactory.get_target_table_resource_from_config_helper(self.config_helper,
                                                                                              self.region)

        print("Exporting from Source")
        source_table.unload_data(self.s3_details)

        print("Importing to Target")
        destination_table.copy_data(self.s3_details)

        if self.s3_details.deleteOnSuccess:
            self.s3_helper.delete_s3_prefix(self.s3_details)


def main(args):
    if len(args) != 3:
        usage()
    else:
        # Legacy mode
        global region
        region = args[2]
        input_config_file = args[1]

        UnloadCopyTool(input_config_file, region)

if __name__ == "__main__":
    main(sys.argv)