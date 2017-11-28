#!/usr/bin/env python
"""
Unittests can only be ran in python3 due to dependencies

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
"""
from unittest import TestCase
from unittest.mock import MagicMock
from util.RedshiftCluster import RedshiftCluster
import redshift_unload_copy
import datetime
import time


class TestRedshiftUnloadCopy(TestCase):
    bucket_name = 'pvb-cloud-storage'
    dir_key = 'Pictures'
    object_key = 'index.html'

    def setUp(self):
        connection_mock = MagicMock()
        redshift_unload_copy.conn_to_rs = MagicMock(return_value=connection_mock)

    def test_region_extract_example_url(self):
        example_url = 'my-cluster.a1bcdefghijk.eu-west-1.redshift.amazonaws.com'
        rs_cluster = RedshiftCluster(example_url)
        self.assertEqual('eu-west-1', rs_cluster.get_region_name())

    def test_region_extract_example_bad_cased_url(self):
        example_url = 'my-cluSter.a1bcdefghijk.eU-west-1.redshift.amazonaws.com'
        rs_cluster = RedshiftCluster(example_url)
        self.assertEqual('eu-west-1', rs_cluster.get_region_name())

    def test_identifier_extract_example_url(self):
        example_url = 'my-cluster.a1bcdefghijk.eu-west-1.redshift.amazonaws.com'
        rs_cluster = RedshiftCluster(example_url)
        self.assertEqual('my-cluster', rs_cluster.get_cluster_identifier())

    def test_identifier_extract_example_bad_cased_url(self):
        example_url = 'my-cluSter.a1bcdefghijk.eU-west-1.redshift.amazonaws.com'
        rs_cluster = RedshiftCluster(example_url)
        self.assertEqual('my-cluster', rs_cluster.get_cluster_identifier())

    def test_temporary_credential_expiration_predicate(self):
        example_url = 'my-cluSter.a1bcdefghijk.eU-west-1.redshift.amazonaws.com'
        rs_cluster = RedshiftCluster(example_url)
        rs_cluster._user_creds_expiration = (datetime.datetime.now() + datetime.timedelta(minutes=1, milliseconds=300))
        self.assertFalse(rs_cluster.is_temporary_credential_expired())
        time.sleep(0.4)
        self.assertTrue(rs_cluster.is_temporary_credential_expired())
