import json
import os
import boto3


class StackParametersBuilder:
    def __init__(self, resource_dir):
        self.resource_dir = resource_dir
        self.parameters = {}
        self.redshift_client = None
        self.parameters_fetched_from_stack_details = False
        self.kms_encrypted_password_fetched_from_password_kms_txt = False
        self.are_s3_parameters_enriched = False
        self.are_cluster_parameters_enriched = False

    def get_redshift_client(self):
        if self.redshift_client is None:
            self.get_parameters_from_stack_details()
            self.redshift_client = boto3.client('redshift', region_name=self.parameters['Region'])
        return self.redshift_client

    def get_parameters_from_stack_details(self):
        if not self.parameters_fetched_from_stack_details:
            stack_details_path = '{resource_dir}/STACK_DETAILS.json'.format(resource_dir=self.resource_dir)
            with open(stack_details_path, 'r') as stack_details_json:
                stack_details = json.load(stack_details_json)
                outputs = stack_details['Stacks'][0]['Outputs']
                for output in outputs:
                    key = output['OutputKey']
                    value = output['OutputValue']
                    self.parameters[key] = value

                self.parameters['Region'] = stack_details['Stacks'][0]['StackId'].split(':')[3]
            self.parameters_fetched_from_stack_details = True

    def get_kms_encrypted_password_from_password_kms_txt(self):
        if not self.kms_encrypted_password_fetched_from_password_kms_txt:
            password_kms_txt = '{resource_dir}/PASSWORD_KMS.txt'.format(resource_dir=self.resource_dir)
            with open(password_kms_txt, 'r') as kms_encrypted_password_file:
                self.parameters['KMSEncryptedPassword']=kms_encrypted_password_file.readline()
            self.kms_encrypted_password_fetched_from_password_kms_txt = True

    def enrich_s3_parameters(self):
        if not self.are_s3_parameters_enriched:
            bucket_arn = self.parameters['S3CopyUnloadBucketArn']
            self.parameters['CopyUnloadBucket'] = bucket_arn.replace('arn:aws:s3:::', '')
            self.are_s3_parameters_enriched = True

    def enrich_cluster_parameters(self):
        if not self.are_cluster_parameters_enriched:
            for cluster in ['SourceCluster', 'TargetCluster']:
                cluster_name = self.parameters[cluster+'Name']
                cluster_describe_response = self.get_redshift_client().describe_clusters(ClusterIdentifier=cluster_name)
                if 'Clusters' not in cluster_describe_response or len(cluster_describe_response['Clusters']) != 1:
                    raise Exception('Could not get details for {type} cluster'.format(type=cluster))
                for parameter in ['DBName', 'MasterUsername', 'Endpoint.Address', 'Endpoint.Port']:
                    parameter_parts = parameter.split('.')
                    value = cluster_describe_response['Clusters'][0]
                    for parameter_part in parameter_parts:
                        value = value.get(parameter_part)
                    parameter_parts.insert(0, cluster)
                    self.parameters[''.join(parameter_parts)] = str(value)
            self.are_cluster_parameters_enriched = True

    def get_parameters_dict(self):
        self.get_parameters_from_stack_details()
        self.get_kms_encrypted_password_from_password_kms_txt()
        self.enrich_s3_parameters()
        self.enrich_cluster_parameters()
        return self.parameters

if __name__ == '__main__':
    default_dir = os.environ['HOME']
    persist_file_target = '{dir}/stack_parameters.json'.format(dir=default_dir)
    if os.path.isfile(persist_file_target):
        with open(persist_file_target, 'r') as stack_parameters:
            parameters = json.load(stack_parameters)
    else:
        pb = StackParametersBuilder(default_dir)
        parameters = pb.get_parameters_dict()
        with open(persist_file_target, 'w') as stack_parameters:
            json.dump(parameters, stack_parameters)
    print(json.dumps(parameters))
