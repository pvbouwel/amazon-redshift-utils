#!/usr/bin/env python
"""
Unittests can only be ran in python3 due to dependencies

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
from unittest import TestCase
from unittest.mock import MagicMock
import redshift_unload_copy
from copy import deepcopy
import boto3


class TestRedshiftUnloadCopy(TestCase):
    test_config = 's3://support-peter-ie/config_test.json'
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
        redshift_unload_copy.main(['fake_app_name','example/config_test.json', 'us-east-1'])
        config = None
        redshift_unload_copy.getConfig(self.test_config)
        s3_config = deepcopy(config)
        redshift_unload_copy.getConfig(self.test_local_config)
        self.assertEqual(s3_config, config)

    def test_decoding_to_verify_kms_client(self):
        redshift_unload_copy.main(['fake_app_name','example/config_test.json', 'us-east-1'])
        encoded = "AQICAHjX2Xlvwj8LO0wam2pvdxf/icSW7G30w7SjtJA5higfdwG7KjYEDZ+jXA6QTjJY9PlDAAAAZTBjBgkqhkiG9w0BBwagVjBUAgEAME8GCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMx+xGf9Ys58uvtfl5AgEQgCILmeoTmmo+Sh1cFgjyqNrySDfQgPYsEYjDTe6OHT5Z0eop"
        decoded_kms = redshift_unload_copy.decrypt(encoded)
        self.assertEqual("testing".encode('utf-8'), decoded_kms)

    @staticmethod
    def unload_data_mock(src_conn, s3_access_credentials, master_symmetric_key, dataStagingPath,
                         src_schema, src_table):
        s3_parts = redshift_unload_copy.tokeniseS3Path(dataStagingPath)
        s3_client = boto3.client('s3', 'eu-west-1')
        response = s3_client.put_object(Body='content1'.encode('utf-8'),
                                        Bucket=s3_parts[0],
                                        Key=s3_parts[1] + 'test_file_1')
        response = s3_client.put_object(Body='content2'.encode('utf-8'),
                                        Bucket=s3_parts[0],
                                        Key=s3_parts[1] + 'test_file_2')

    def test_staging_area_should_be_cleaned_up_when_delete_on_success(self):
        s3_client = boto3.client('s3', 'eu-west-1')
        mock_unload = MagicMock(side_effect=TestRedshiftUnloadCopy.unload_data_mock,
                                                     return_value=MagicMock())
        redshift_unload_copy.unload_data = mock_unload
        redshift_unload_copy.main(['fake_app_name','example/config_test.json', 'us-east-1'])
        full_s3_path = mock_unload.call_args[0][3]
        prefix = '/'.join(full_s3_path.split('/')[3:])
        objects = s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        self.assertEqual(objects['KeyCount'], 0)


