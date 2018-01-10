from unittest import TestCase

from util.sql.ddl_generators import SQLTextHelper


class TableDDLHelperTests(TestCase):
    def test_remove_simple_block(self):
        input_sql = """/*  co
               mm
                 ents
           can make -- things more difficult */ select 1;"""
        expected_sql = " select 1;"
        self.assertEquals(expected_sql, SQLTextHelper.get_sql_without_comments(input_sql_text=input_sql))

    def test_remove_simple_line_comment(self):
        input_sql = """-- things more difficult
--More SQL        
 select 1;"""
        expected_sql = " select 1;"
        self.assertEquals(expected_sql, SQLTextHelper.get_sql_without_comments(input_sql_text=input_sql))

    def test_remove_line_comment_must_not_influence_string_literals(self):
        input_sql = """select '--DROP TABLE "';"""
        expected_sql = input_sql
        self.assertEquals(expected_sql, SQLTextHelper.get_sql_without_comments(input_sql_text=input_sql))

    def test_remove_2_line_comments(self):
        input_sql = """-- things more difficult
--More SQL        
 select 1
union all
 select 2;"""
        expected_sql = """ select 1
union all
 select 2;"""
        self.assertEquals(expected_sql, SQLTextHelper.get_sql_without_comments(input_sql_text=input_sql))

    def test_remove_2_line_comments_and_white_spaces(self):
        input_sql = """-- things more difficult
--More SQL        
 select 1
union all
 select 2;"""
        expected_sql = """select 1 union all select 2;"""
        result_sql = SQLTextHelper.get_sql_without_commands_newlines_and_whitespace(input_sql_text=input_sql)
        self.assertEquals(expected_sql, result_sql)

    def test_remove_2_line_comments_and_white_spaces_string_literals(self):
        input_sql = """-- things more difficult
--More SQL        
 select '1   2'
union all
 select 2;"""
        expected_sql = """select '1   2' union all select 2;"""
        result_sql = SQLTextHelper.get_sql_without_commands_newlines_and_whitespace(input_sql_text=input_sql)
        self.assertEquals(expected_sql, result_sql)
