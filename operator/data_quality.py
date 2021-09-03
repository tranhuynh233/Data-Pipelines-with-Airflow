from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 check_queries=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_queries = check_queries


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for query in self.check_queries:
            result = int(redshift.get_first(sql=query['sql'])[0])
            if query['operator'] == '==':
                if result != query['value']:
                    raise AssertionError(f"Failed: {result} not equal to expected result {query['value']}")
            if query['operator'] == '>':
                if result <= query['value']:
                    raise AssertionError(f"Failed: {result} not greater than expected result {query['value']}")
            if query['operator'] == '<':
                if result >= query['value']:
                    raise AssertionError(f"Failed: {result} not less than expected result {query['value']}")
            if query['operator'] == '!=':
                if result == query['value']:
                    raise AssertionError(f"Failed: {result} not different from expected result {query['value']}")

            self.log.info(f"Passed: {result} matched expected result")
