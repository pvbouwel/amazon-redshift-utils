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
import logging
from global_config import GlobalConfigParametersReader, config_parameters
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
    EXIT_CODE_TABLE_NOT_EXIST_AND_NO_AUTO_CREATE = 500
    EXIT_CODE_TABLE_NOT_EXIST_AND_DIFFERENT_TABLE_NAME_THAN_SOURCE = 599
    EXIT_CODE_TABLE_NOT_EXIST_AND_DIFFERENT_SCHEMA_NAME_THAN_SOURCE = 599

    # noinspection PyDefaultArgument
    def __init__(self,
                 config_file,
                 region_name,
                 global_config_values=GlobalConfigParametersReader().get_default_config_key_values()):
        for key, value in global_config_values.items():
            config_parameters[key] = value
        self.region = region_name
        self.s3_helper = S3Helper(self.region)

        # load the configuration
        self.config_helper = ConfigHelper(config_file, self.s3_helper)

        self.source_table = TableResourceFactory.get_source_table_resource_from_config_helper(self.config_helper,
                                                                                              self.region)

        self.destination_table = TableResourceFactory.get_target_table_resource_from_config_helper(self.config_helper,
                                                                                                   self.region)

        if global_config_values['destinationTablePreTest'] \
                or global_config_values['destinationTableAutoCreate'] \
                or global_config_values['destinationSchemaAutoCreate'] \
                or global_config_values['destinationTableForceDropCreate']:
            if self.destination_table.is_present():
                if global_config_values['destinationTableAutoCreate'] \
                        and not global_config_values['destinationTableForceDropCreate']:
                    logging.info('Destination table {s}.{t} already exists, ignoring auto-create.'.format(
                        s=self.destination_table.get_schema(),
                        t=self.destination_table.get_table()
                    ))
                if global_config_values['destinationTableForceDropCreate']:
                    self.destination_table.drop()
            try:
                self.destination_table.clone_structure_from(self.source_table)
                logging.info('Creating target table {tbl}'.format(tbl=str(self.destination_table)))
                self.destination_table.create()
            except Exception as e:
                logging.fatal(str(e))
                sys.exit(1)

        self.s3_details = S3Details(self.config_helper, self.source_table, encryptionKeyID=encryptionKeyID)

        logging.info("Exporting from Source")
        self.source_table.unload_data(self.s3_details)

        logging.info("Importing to Target")
        self.destination_table.copy_data(self.s3_details)

        if self.s3_details.deleteOnSuccess:
            self.s3_helper.delete_s3_prefix(self.s3_details)


def set_log_level(log_level_string):
    log_level_string = log_level_string.upper()
    if not hasattr(logging, log_level_string):
        logging.error('Could not find log_level {lvl}'.format(lvl=log_level_string))
        logging.basicConfig(level=logging.INFO)
    else:
        stdout_handler = logging.StreamHandler(stream=sys.stdout)
        stdout_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
            '%m-%d %H:%M:%S'
        )
        stderr_handler = logging.StreamHandler()
        stderr_handler.setFormatter(formatter)
        log_level = getattr(logging, log_level_string)
        stderr_handler.setLevel(log_level)
        logging.basicConfig(level=log_level, handlers=[stdout_handler, stderr_handler])
        logging.debug('Log level set to {lvl}'.format(lvl=log_level_string))


def main(args):
    global region

    global_config_reader = GlobalConfigParametersReader()
    global_config_values = global_config_reader.get_config_key_values_updated_with_cli_args(args)
    set_log_level(global_config_values['logLevel'])

    UnloadCopyTool(global_config_values['s3ConfigFile'], global_config_values['region'], global_config_values)

if __name__ == "__main__":
    main(sys.argv)
