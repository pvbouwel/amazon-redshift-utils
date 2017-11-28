#!/usr/bin/env python
"""
* Copyright 2017, Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

Unittests can only be ran in python3 due to dependencies

"""
from unittest import TestCase
from unittest.mock import MagicMock, patch, call
import redshift_unload_copy
import boto3

import util.RedshiftCluster

import util.KMSHelper
import util.S3Utils


class TestRedshiftUnloadCopy(TestCase):
    s3_test_config = 's3://support-peter-ie/config_test.json'
    test_local_config = 'example/config_test.json'
    bucket_name = 'support-peter-ie'
    s3_path_prefix = 'tests'

    def get_s3_key_for_object_name(self, object_name):
        return self.s3_path_prefix + '/' + object_name

    def setUp(self):
        redshift_unload_copy.conn_to_rs = MagicMock(return_value=MagicMock())
        redshift_unload_copy.copy_data = MagicMock(return_value=MagicMock())
        redshift_unload_copy.unload_data = MagicMock(return_value=MagicMock())

    def test_config_local_is_same_as_using_S3(self):
        """
        Mostly to test S3 boto to boto3 change
        :return:
        """
        s3_helper = util.S3Utils.S3Helper('eu-west-1')
        s3_config = redshift_unload_copy.ConfigHelper(self.s3_test_config, s3_helper).config
        local_config = redshift_unload_copy.ConfigHelper(self.test_local_config).config
        self.assertEqual(s3_config, local_config)

    def test_decoding_to_verify_kms_client(self):
        kms_helper = util.KMSHelper.KMSHelper('us-east-1')
        encoded = "AQICAHjX2Xlvwj8LO0wam2pvdxf/icSW7G30w7SjtJA5higfdwG7KjYEDZ+jXA6QTjJY9PlDAAAAZTBjBgkqhkiG9w0BBwagVjBUAgEAME8GCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMx+xGf9Ys58uvtfl5AgEQgCILmeoTmmo+Sh1cFgjyqNrySDfQgPYsEYjDTe6OHT5Z0eop"
        decoded_kms = kms_helper.decrypt(encoded)
        self.assertEqual("testing".encode('utf-8'), decoded_kms)

    class TableResourceMock:
        def __init__(self, rs_cluster, schema, table):
            self._cluster = rs_cluster
            self._schema = schema
            self._table = table
            self.dataStagingPath = None

        def get_db(self):
            return self._cluster.get_db()

        def get_schema(self):
            return self._schema

        def get_table(self):
            return self._table

        def unload_data(self, s3_details):
            s3_parts = util.S3Utils.S3Helper.tokenise_S3_Path(s3_details.dataStagingPath)
            s3_client = boto3.client('s3', 'eu-west-1')
            s3_client.put_object(Body='content1'.encode('utf-8'),
                                 Bucket=s3_parts[0],
                                 Key=s3_parts[1] + 'test_file_1')
            s3_client.put_object(Body='content2'.encode('utf-8'),
                                 Bucket=s3_parts[0],
                                 Key=s3_parts[1] + 'test_file_2')
            self.dataStagingPath = s3_details.dataStagingPath

        def copy_data(self, s3_details):
            pass

    def test_staging_area_should_be_cleaned_up_when_delete_on_success(self):
        s3_client = boto3.client('s3', 'eu-west-1')
        with patch('util.TableResource.TableResource',
                   new=TestRedshiftUnloadCopy.TableResourceMock) as unload_mock:
                uct = redshift_unload_copy.UnloadCopyTool('example/config_test.json', 'us-east-1')
                full_s3_path = uct.s3_details.dataStagingPath
        prefix = '/'.join(full_s3_path.split('/')[3:])
        objects = s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        self.assertEqual(objects['KeyCount'], 0)

    def test_cluster_commands_for_test_config(self):
        kms_mock = MagicMock(return_value='Eh39yqNUt2BgQMluXqI89Oz1ydvthaatSIm8B5kwMz0=')
        execute_query_mock = MagicMock()
        util.KMSHelper.KMSHelper.generate_base64_encoded_data_key = kms_mock
        util.RedshiftCluster.RedshiftCluster._conn_to_rs = MagicMock()
        util.RedshiftCluster.RedshiftCluster.execute_query = execute_query_mock
        util.RedshiftCluster.RedshiftCluster._disconnect_from_rs = MagicMock()

        uct = redshift_unload_copy.UnloadCopyTool('example/config_test.json', 'us-east-1')

        unload_command = """unload ('SELECT * FROM public.export_table')
                     to 's3://support-peter-ie/tests/{now}/mydb.public.export_table.' credentials 
                     'aws_iam_role=aws iam role which is assigned to Redshift and has access to the s3 bucket;master_symmetric_key=Eh39yqNUt2BgQMluXqI89Oz1ydvthaatSIm8B5kwMz0='
                     manifest
                     encrypted
                     gzip
                     delimiter '^' addquotes escape allowoverwrite""".format(now=uct.s3_details.nowString)

        copy_command = """copy public.import_table
                   from 's3://support-peter-ie/tests/{now}/mydb.public.export_table.manifest' credentials 
                   'aws_iam_role=aws iam role which is assigned to Redshift and has access to the s3 bucket;master_symmetric_key=Eh39yqNUt2BgQMluXqI89Oz1ydvthaatSIm8B5kwMz0='
                   manifest 
                   encrypted
                   gzip
                   delimiter '^' removequotes escape compupdate off REGION 'us-east-1' """.format(now=uct.s3_details.nowString)
        calls = [call(unload_command), call(copy_command)]
        execute_query_mock.assert_has_calls(calls)


