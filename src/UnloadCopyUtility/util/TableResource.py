import logging

from util.KMSHelper import KMSHelper
from util.RedshiftCluster import RedshiftCluster


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
                     to '{dataStagingPath}.' credentials 
                     '{s3_access_credentials};master_symmetric_key={master_symmetric_key}'
                     manifest
                     encrypted
                     gzip
                     delimiter '^' addquotes escape allowoverwrite"""
    commands['unload'] = unload_stmt

    copy_stmt = """copy {schema_name}.{table_name}
                   from '{dataStagingPath}.manifest' credentials 
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

    def get_db(self):
        return self._cluster.get_db()

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