import re
from abc import abstractmethod

from util.child_object import ChildObject
from util.kms_helper import KMSHelper
from util.redshift_cluster import RedshiftCluster
from util.sql.ddl_generators import SchemaDDLHelper, TableDDLHelper, TableDDLTransformer
from util.sql_queries import GET_DATABASE_NAME_OWNER_ACL, GET_SCHEMA_NAME_OWNER_ACL, GET_TABLE_NAME_OWNER_ACL


class Resource:
    def __init__(self):
        self.create_sql = None

    @abstractmethod
    def get_statement_to_retrieve_ddl_create_statement_text(self):
        pass

    def get_create_sql(self, generate=False):
        if generate:
            ddl_dict = self.get_cluster().get_query_full_result_as_list_of_dict(
                self.get_statement_to_retrieve_ddl_create_statement_text()
            )
            ddl = '\n'.join([r['ddl'] for r in ddl_dict])
            self.create_sql = ddl_dict
            return ddl
        else:
            if self.create_sql is not None:
                return self.create_sql
            else:
                raise Resource.CreateSQLNotSet('No create sql configured for resource {r}'.format(r=str(self)))

    def set_create_sql(self, create_sql):
        self.create_sql = create_sql

    @abstractmethod
    def get_cluster(self):
        pass

    def create(self, sql_text = None):
        if sql_text is None:
            sql_text = self.get_create_sql()
        self.get_cluster().execute_update(sql_text)

    @abstractmethod
    def drop(self):
        pass

    @abstractmethod
    def is_present(self, force_update=False):
        pass

    @abstractmethod
    def clone_structure_from(self, other):
        """
        Change DDL of self such that it has the same structure as other
        :param other: Resource implementation of same type a self
        :return:
        """
        pass

    class NotFound(Exception):
        def __init__(self, msg):
            self.msg = msg

        def __str__(self):
            s = str(self) + '\n\t' + self.msg
            return s

    class CreateSQLNotSet(NotFound):
        def __init__(self, msg):
            pass


class DBResource(Resource):
    def __init__(self, rs_cluster):
        """

        :param rs_cluster:
        members:
         - is_present_query: sql query that returns a single row if present otherwise <> 1 row
            this query can use parameters but they should be retrievable from the object as
            get_<parameter_name>()
        """
        Resource.__init__(self)
        self._cluster = rs_cluster
        self.name = None
        self.owner = None
        self.acl = None
        self.get_name_owner_acl_sql = GET_DATABASE_NAME_OWNER_ACL

    def get_db(self):
        return self._cluster.get_db()

    def get_cluster(self):
        return self._cluster

    def set_cluster(self, cluster):
        self._cluster = cluster

    def __eq__(self, other):
        return type(self) == type(other) and \
               self.get_db() == other.get_db() and \
               self.get_cluster() == other.get_cluster()

    def get_query_sql_text_with_parameters_replaced(self, sql_text):
        param_dict = {}
        for match_group in re.finditer(r'({[^}{]*})', sql_text):
            parameter_name = match_group.group().lstrip('{').rstrip('}')
            method = getattr(self, 'get_' + parameter_name)
            param_dict[parameter_name] = method()

        return sql_text.format(**param_dict)

    def retrieve_name_owner_acl_and_store_in_resource(self, force_update=False):
        if self.name is None or force_update:
            self.name = self.owner = self.acl = None
            get_details_sql = self.get_query_sql_text_with_parameters_replaced(self.get_name_owner_acl_sql)
            result = self.get_cluster().get_query_full_result_as_list_of_dict(get_details_sql)
            if len(result) == 0:
                raise Resource.NotFound('Resource {r} not found!'.format(r=str(self)))
            if len(result) > 1:
                raise Resource.NotFound('Multiple rows when retrieving Resource {r}'.format(r=str(self)))
            self.name = result[0]['name']
            self.owner = result[0]['owner']
            self.acl = result[0]['acl']

    def is_present(self, force_update=False):
        try:
            self.retrieve_name_owner_acl_and_store_in_resource(force_update=force_update)
        except Resource.NotFound:
            return False
        return self.name is not None


class SchemaResource(DBResource, ChildObject):
    def __init__(self, rs_cluster, schema):
        DBResource.__init__(self, rs_cluster)
        self._schema = schema
        self.get_name_owner_acl_sql = GET_SCHEMA_NAME_OWNER_ACL

    def get_schema(self):
        return self._schema

    def set_schema(self, schema):
        self._schema = schema

    def __eq__(self, other):
        return type(self) == type(other) and \
               self.get_schema() == other.get_schema() and \
               DBResource.__eq__(self, other)

    def get_statement_to_retrieve_ddl_create_statement_text(self):
        return SchemaDDLHelper().get_schema_ddl_SQL(schema_name=self.get_schema())

    # TODO: clone_structure_from


class TableResource(SchemaResource):

    def get_statement_to_retrieve_ddl_create_statement_text(self):
        return TableDDLHelper().get_table_ddl_SQL(table_name=self.get_table(), schema_name=self.get_schema())

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

    def get_table(self):
        return self._table

    def set_table(self, table):
        self._table = table

    def __init__(self, rs_cluster, schema, table):
        SchemaResource.__init__(self, rs_cluster, schema)
        self._table = table
        self.get_name_owner_acl_sql = GET_TABLE_NAME_OWNER_ACL

    def __eq__(self, other):
        return type(self) == type(other) and \
               self.get_table() == other.get_table() and \
               SchemaResource.__eq__(self, other)

    def run_command_against_table_resource(self, command, command_parameters):
        command_parameters['schema_name'] = self.get_schema()
        command_parameters['table_name'] = self.get_table()
        command_parameters['cluster'] = self.get_cluster()
        command_to_execute = self.commands[command]
        if 'region' in command_parameters and command == 'copy' and command_parameters['region'] is not None:
            command_to_execute += " REGION '{region}' "
        self.get_cluster().execute_update(command_to_execute.format(**command_parameters))

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

    def clone_structure_from(self, other):
        other_ddl = other.get_create_sql(generate=True)
        self.set_create_sql(TableDDLTransformer.get_create_table_ddl_for_different_relation(
            other_ddl,
            new_table_name=self.get_table(),
            new_schema_name=self.get_schema()
        ))


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