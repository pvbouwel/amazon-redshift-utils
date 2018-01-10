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

import json
import sys

from global_config import GlobalConfigParametersReader
from util.s3_utils import S3Helper, S3Details
from util.resources import TableResourceFactory

region = None

encryptionKeyID = 'alias/RedshiftUnloadCopyUtility'


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


class UnloadCopyTool:
    def __init__(self, config_file, region, global_config_values={}):
        self.region = region
        self.s3_helper = S3Helper(self.region)

        # load the configuration
        self.config_helper = ConfigHelper(config_file, self.s3_helper)


        self.source_table = TableResourceFactory.get_source_table_resource_from_config_helper(self.config_helper,
                                                                                              self.region)
        self.destination_table = TableResourceFactory.get_target_table_resource_from_config_helper(self.config_helper,
                                                                                                   self.region)

        self.s3_details = S3Details(self.config_helper, self.source_table, encryptionKeyID=encryptionKeyID)

        print("Exporting from Source")
        self.source_table.unload_data(self.s3_details)

        print("Importing to Target")
        self.destination_table.copy_data(self.s3_details)

        if self.s3_details.deleteOnSuccess:
            self.s3_helper.delete_s3_prefix(self.s3_details)




def main(args):
    global region
    input_config_file = None
    global_config_values = None

    if len(args) != 3:
        global_config_reader = GlobalConfigParametersReader()
        global_config_values = global_config_reader.get_config_key_values_updated_with_cli_args(args)
        counter = 1
        if 's3ConfigFile' in global_config_values and global_config_values['s3ConfigFile'] is not None:
            input_config_file = global_config_values['s3ConfigFile']
        else:
            input_config_file = args[counter]
            counter += 1

        if 'region' in global_config_values and global_config_values['region'] is not None:
            region = global_config_values['region']
        else:
            region = args[counter]
            counter += 1

        if len(global_config_reader.unprocessed_arguments) != counter:
            usage()
    else:
        # Legacy mode
        region = args[2]
        input_config_file = args[1]
        global_config_reader = GlobalConfigParametersReader()
        global_config_values = global_config_reader.get_config_key_values_updated_with_cli_args(args)

    UnloadCopyTool(input_config_file, region, global_config_values)

if __name__ == "__main__":
    main(sys.argv)