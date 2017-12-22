import boto3
import os
import random
import string
import json

cloudformation = boto3.client('cloudformation', region_name='eu-west-1')

properties_file = 'stack_parameter_overrides.txt'
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

random_suffix = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for i in range(8))
stack_name = 'unload-copy-{r}'.format(r=random_suffix)

cloudformation_file = 'RedshiftCFTemplate.json'
if os.path.exists(cloudformation_file) and os.path.isfile(cloudformation_file):
    with open(cloudformation_file, 'r') as cloudformation_file_content:
        CFBody = cloudformation_file_content.read()

CFBody = json.dumps(json.loads(CFBody), separators=(',', ':'))

cloudformation.create_stack(StackName=stack_name, TemplateBody=CFBody, Parameters=stack_parameters,
                            Capabilities=['CAPABILITY_IAM'])
