from util.sql.sql_text_helpers import SQLTextHelper
import re
import logging


class DDLHelper:
    def __init__(self, path_to_v_generate, view_start):
        with open(path_to_v_generate, 'r') as v_generate:
            self.view_sql = v_generate.read()
        self.view_sql = SQLTextHelper.get_sql_without_commands_newlines_and_whitespace(self.view_sql)
        if self.view_sql.startswith(view_start):
            self.view_query_sql = self.view_sql[len(view_start):]
        self.filter_sql = ''

    def get_sql(self):
        return SQLTextHelper.remove_trailing_semicolon(self.view_query_sql) + self.filter_sql + ';'

    def add_filters(self, filters):
        filter_list = []
        for filter_name in filters.keys():
            if type(filters[filter_name]) in (type(int),  type(float)):
                filter_list.append("{key}={value}".format(key=filter_name, value=filters[filter_name]))
            else:
                filter_list.append("{key}='{value}'".format(key=filter_name, value=filters[filter_name]))
        if len(filter_list) > 0:
            self.filter_sql = ' WHERE '
            self.filter_sql += ' AND '.join(filter_list)
        else:
            self.filter_sql = ''


class TableDDLHelper(DDLHelper):
    def __init__(self, path_to_v_generate_table_ddl='./../AdminViews/v_generate_tbl_ddl.sql'):
        view_start = 'CREATE OR REPLACE VIEW admin.v_generate_tbl_ddl AS '
        DDLHelper.__init__(self, path_to_v_generate_table_ddl, view_start)

    # noinspection PyPep8Naming
    def get_table_ddl_SQL(self, table_name=None, schema_name=None):
        filters = {}
        if table_name is not None:
            filters['tablename'] = table_name
        if schema_name is not None:
            filters['schemaname'] = schema_name
        self.add_filters(filters)
        return self.get_sql()


class DDLTransformer:
    def __init__(self):
        pass

    @staticmethod
    def get_ddl_for_different_relation(ddl, new_table_name=None, new_schema_name=None):
        clean_ddl = SQLTextHelper.get_sql_without_commands_newlines_and_whitespace(ddl)
        if clean_ddl.lower().startswith('CREATE TABLE IF NOT EXISTS "'):
            return TableDDLTransformer.get_create_table_ddl_for_different_relation(
                clean_ddl,
                new_table_name=new_table_name,
                new_schema_name=new_schema_name
            )


class TableDDLTransformer:
    def __init__(self):
        pass

    @staticmethod
    def get_create_table_ddl_for_different_relation(ddl, new_table_name=None, new_schema_name=None):
        """
        Get ddl but adapt it to create a relation with different name but same structure
        :param ddl:  ddl from admin.v_generate_tbl_ddl view
        :param new_table_name: if None don't replace table_name
        :param new_schema_name: if None don't replace schema_name
        :return:
        """
        try:
            round_bracket_separated_parts = ddl.split('(')
            first_round_bracket_part = round_bracket_separated_parts[0]
            space_separated_parts = first_round_bracket_part.split(' ')
            relation_specification = space_separated_parts[-1]
            match_dict = re.match(r'"(?P<schema_name>.*)"\."(?P<table_name>.*)"', relation_specification).groupdict()
            original_table_name = match_dict['table_name']
            original_schema_name = match_dict['schema_name']
            new_table_name = new_table_name or original_table_name
            new_schema_name = new_schema_name or original_schema_name
            relation_specification = '"{schema}"."{table}"'.format(
                schema=new_schema_name.replace('"', '""'),  # In SQL we need to escape double quotes
                table=new_table_name.replace('"', '""')
            )
            space_separated_parts[-1] = relation_specification
            round_bracket_separated_parts[0] = ' '.join(space_separated_parts)
            new_ddl = '('.join(round_bracket_separated_parts)
        except:
            logging.debug('Clean ddl: {ddl}\nRelation name: {rel_name}'.format(
                ddl=ddl,
                rel_name=relation_specification
            ))
            raise TableDDLTransformer.InvalidDDLSQLException(ddl)
        return new_ddl

    class InvalidDDLSQLException(Exception):
        def __init__(self, ddl):
            super(TableDDLTransformer.InvalidDDLSQLException, self).__init__()
            self.ddl = ddl


class SchemaDDLHelper(DDLHelper):
    def __init__(self, path_to_v_generate_table_ddl='./../AdminViews/v_generate_schema_ddl.sql'):
        view_start = 'CREATE OR REPLACE VIEW admin.v_generate_schema_ddl AS '
        DDLHelper.__init__(self, path_to_v_generate_table_ddl, view_start)

    # noinspection PyPep8Naming
    def get_schema_ddl_SQL(self, schema_name=None):
        filters = {}
        if schema_name is not None:
            filters['schemaname'] = schema_name
        self.add_filters(filters)
        return self.get_sql()
