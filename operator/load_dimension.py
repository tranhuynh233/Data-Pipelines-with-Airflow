from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 columns="",
                 sql="",
                 append_only="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.columns=columns
        self.sql=sql
        self.append_only=append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Loading {} table'.format(self.table))
        
        if self.append_only == True:
            insert_stmt = "INSERT INTO {}{} {}".format(self.table,self.columns,self.sql)
            redshift.run(self.sql)
        else:
            delete_stmt = "DELETE FROM {}".format(self.table)
            redshift.run(delete_stmt)
            insert_stmt = "INSERT INTO {}{} {}".format(self.table,self.columns,self.sql)
            redshift.run(insert_stmt)
