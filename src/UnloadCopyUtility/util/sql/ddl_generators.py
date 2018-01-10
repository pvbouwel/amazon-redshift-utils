from util.sql.sql_text_helpers import SQLTextHelper


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


