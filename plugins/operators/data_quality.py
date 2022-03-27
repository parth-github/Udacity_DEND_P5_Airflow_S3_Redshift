import re
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        dq_checks=[
            {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null;", 'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null;", 'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null;", 'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid is null OR start_time is null;", 'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null;", 'expected_result': 0}
        ]
        for entry in dq_checks:
            
            rows = redshift_hook.get_records(entry.get('check_sql'))
            #self.assertEqual(rows[0][0], entry.get('expected_result'), f"Data Quality validation failed for table : {entry.get('check_sql')}.")

            if rows[0][0] != entry.get('expected_result'):
                self.log.error(f"Data Quality validation failed for table : {entry.get('check_sql')}.")
                raise ValueError(f"Data Quality validation failed for table : {entry.get('check_sql')}.")
        
        for table in self.tables:
            self.log.info(f"Starting data quality validation on table : {table}")
            records = redshift_hook.get_records(f"select count(*) from {table};")

            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"Data Quality validation failed for table : {table}.")
                raise ValueError(f"Data Quality validation failed for table : {table}")
            self.log.info(f"Data Quality Validation Passed on table : {table}!!!")            
            
            
        
        self.log.info('DataQualityOperator not implemented yet')