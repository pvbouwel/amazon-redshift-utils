import boto3
from botocore.exceptions import ClientError
import os
import random
import string
import json
import time
import logging
import sys
from copy import deepcopy
import datetime
import paramiko

os.tmpfile()

cloudformation = boto3.client('cloudformation', region_name='eu-west-1')
logging.basicConfig(level=logging.INFO)


class CloudFormationStack:
    POLL_INTERVAL = 60

    def __init__(self, stack_name):
        self.stack_name = self.get_name(stack_name)
        self.stack_details = None
        self.stack_details_update_date = datetime.datetime.now() - datetime.timedelta(minutes=15)

    def get_name(self, stack_name=None):
        if hasattr(self, 'stack_name') and self.stack_name is not None:
            return self.stack_name
        else:
            stack_name_parts = ['unload-copy']
            if stack_name is not None:
                stack_name_parts.append(stack_name)
            return '-'.join(stack_name_parts)

    def _refresh_stack_details(self, force=False, initial=False):
        stack_name = self.get_name()

        if force or not self.stack_details_update_date + datetime.timedelta(seconds=CloudFormationStack.POLL_INTERVAL) > datetime.datetime.now():
            try:
                response = cloudformation.describe_stacks(StackName=stack_name)
            except ClientError as e:
                if e.response['Error']['Code'] == 'ValidationError' \
                        and e.response['Error']['Message'].startswith('Stack with id ') \
                        and e.response['Error']['Message'].endswith('does not exist'):
                    self.stack_details = {'StackStatus': 'NON-EXISTENT'}
                    return
            except Exception as e:
                logging.error('Failed to describe stack {sn}: {e}'.format(sn=stack_name, e=str(e)))
                self._refresh_stack_details()
                return
            # noinspection PyUnboundLocalVariable
            if len(response['Stacks']) > 1:
                raise CloudFormationStack.MultipleStacksException()
            else:
                # 'StackStatus': 'CREATE_IN_PROGRESS'|'CREATE_FAILED'|'CREATE_COMPLETE'|'ROLLBACK_IN_PROGRESS'|'ROLLBACK_FAILED'|'ROLLBACK_COMPLETE'|'DELETE_IN_PROGRESS'|'DELETE_FAILED'|'DELETE_COMPLETE'|'UPDATE_IN_PROGRESS'|'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS'|'UPDATE_COMPLETE'|'UPDATE_ROLLBACK_IN_PROGRESS'|'UPDATE_ROLLBACK_FAILED'|'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS'|'UPDATE_ROLLBACK_COMPLETE'|'REVIEW_IN_PROGRESS'
                logging.debug('Stack has status')
                self.stack_details = deepcopy(response['Stacks'][0])
            self.stack_details_update_date = datetime.datetime.now()

    def get_status(self):
        self._refresh_stack_details()
        return self.stack_details['StackStatus']

    def get_output_variable(self, output_variable_name):
        self._refresh_stack_details()
        return self.stack_details['Outputs'][output_variable_name]

    def create_stack(self):
        cloudformation.create_stack(StackName=self.get_name(),
                                    TemplateBody=CloudFormationStack.get_cloud_formation_template_body(),
                                    Parameters=CloudFormationStack.get_cloud_formation_stack_properties(),
                                    Capabilities=['CAPABILITY_IAM'])

    def teardown_stack(self):
        cloudformation.delete_stack(StackName=self.get_name())

    @staticmethod
    def get_cloud_formation_stack_properties(properties_file='stack_parameter_overrides.txt'):
        stack_parameters = []
        if os.path.exists(properties_file) and os.path.isfile(properties_file):
            with open(properties_file, 'r') as properties_file_content:
                for line in properties_file_content:
                    key, value = line.split('=')
                    value = value.rstrip('\n')
                    stack_parameters.append({'ParameterKey': key,
                                             'ParameterValue': value})

        random_suffix = ''.join(random.choice(string.ascii_uppercase + string.digits) for i in range(8))
        stack_parameters.append({'ParameterKey': 'MasterUserPassword',
                                 'ParameterValue': 'Pass1.{r}'.format(r=random_suffix)})
        return stack_parameters

    @staticmethod
    def get_cloud_formation_template_body(cloudformation_file='RedshiftCFTemplate.json'):
        if os.path.exists(cloudformation_file) and os.path.isfile(cloudformation_file):
            with open(cloudformation_file, 'r') as cloudformation_file_content:
                cloudformation_body = cloudformation_file_content.read()

        # noinspection PyUnboundLocalVariable
        cloudformation_body = json.dumps(json.loads(cloudformation_body), separators=(',', ':'))
        return cloudformation_body

    class MultipleStacksException(Exception):
        def __init__(self):
            pass


class StackRunner:
    POLL_INTERVAL = 60

    def __init__(self, arguments=sys.argv):
        self.default_config = {
            'stack_name': None,
            'create': True,
            'auto_delete': False,
            'ssh_key': None
        }

        self.config = deepcopy(self.default_config)
        self.parse_arguments(arguments)
        self.stack = CloudFormationStack(self.config['stack_name'])

        if self.stack.get_status() == 'NON-EXISTENT':
            logging.info('Stack {sn} does not exist.'.format(sn=self.stack.get_name()))
            if self.config['create']:
                logging.info('Creating non-existent stack {sn}'.format(sn=self.stack.get_name()))
                self.stack.create_stack()
                time.sleep(self.stack.POLL_INTERVAL)

        if self.config['auto_delete']:
            if self.stack.get_status() == 'NON-EXISTENT':
                logging.info('Stack {sn} does not exist, cannot delete something that does not exist.'.format(
                    sn=self.stack.get_name()
                ))
                sys.exit(0)
            while not self.is_stack_ready_for_teardown():
                time.sleep(self.stack.POLL_INTERVAL)
            self.stack.teardown_stack()

    def is_stack_ready_for_teardown(self):
        return self.stack.get_status() == 'CREATE_COMPLETE' and self.are_all_tests_completed()

    def are_all_tests_completed(self):
        ec2_instance_url = self.stack.get_output_variable('EC2IP')
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(ec2_instance_url, username='ec2-user', key_filename=self.config['ssh_key'])
            stdin, stdout, stderr = client.exec_command('tail ${HOME}/STATUS')

            result_text = stdout.read()
            logging.debug('Status ended with:' + str(result_text))
        finally:
            client.close()
        if 'STATUS=Complete' in result_text:
            return True
        return False

    def parse_arguments(self, arguments):
        key_name = None
        for argument in arguments[1:]:
            if self.is_boolean_config(argument):
                self.set_boolean_config(cli_argument=argument)
            elif argument.startswith('--'):
                if argument.lower() == 'ssh-key':
                    key_name = 'ssh_key'
                else:
                    logging.warning('Encountered unknown argument {arg}'.format(arg=argument))
                    self.print_supported_arguments()
                    sys.exit(88)
            else:
                if key_name is not None:
                    self.config[key_name] = argument
                    key_name = None
                else:
                    if self.config['stack_name'] is not None:
                        raise Exception('Encountered {arg} but stack_name set already to {sn}'.format(
                            arg=argument,
                            sn=self.config['stack_name']
                        ))
                    else:
                        self.config['stack_name'] = argument

    def is_boolean_config(self, cli_argument):
        cli_argument = StackRunner.cli_argument_to_config_name(cli_argument)
        if cli_argument in self.config and type(self.config[cli_argument]) is bool:
            return True
        return False

    @staticmethod
    def cli_argument_to_config_name(cli_argument):
        if cli_argument.startswith('--'):
            cli_argument = cli_argument[2:]
        if cli_argument.startswith('no-'):
            cli_argument = cli_argument[3:]
        return cli_argument.replace('-', '_')

    def set_boolean_config(self, cli_argument):
        if cli_argument.startswith('--no-'):
            self.config[StackRunner.cli_argument_to_config_name(cli_argument)] = False
        else:
            self.config[StackRunner.cli_argument_to_config_name(cli_argument)] = True

    def print_supported_arguments(self):
        for key in self.config.keys():
            logging.info('Argument --{arg} has default value {arg_def}'.format(
                arg=key.replace('_', '-'),
                arg_def=self.default_config[key]
            ))

if __name__ == '__main__':
    sr = StackRunner()