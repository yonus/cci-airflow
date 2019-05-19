from airflow.utils.dates import days_ago
from delta.base.voyage_etl_base import VoyageEtlBase
from airflow.contrib.hooks.bigquery_hook import BigQueryHook,BigQueryCursor
class VoyageEtlDeltaCustomer(VoyageEtlBase):

     def __init__(self,dag_id,
        project_id,
        table_name,
        query, 
        gcs_bucket, 
        gbq_dataset,
        gbq_table,
        mssql_conn_id="cci_voyage_sql",
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default",
        bigquery_create_disposition="CREATE_IF_NEEDED",
        bigquery_write_disposition="WRITE_APPEND",
        start_date=days_ago(0),
        schedule_interval=None):
        self.project_id = project_id
        super.__init__(dag_id,project_id,table_name,query,gcs_bucket,gbq_dataset, mssql_conn_id,google_cloud_storage_conn_id,bigquery_conn_id,bigquery_create_disposition,bigquery_write_disposition,start_date,schedule_interval)
        

     def getQueryCriteria(self):
         lastSucessfullEtlTime = self.getLastSuccessfullEtlTime(self.gbq_table)
         if lastSucessfullEtlTime is None:
            return ""
         else:
            " WHERE upDate > " + lastSucessfullEtlTime
         

     
       

      

   

     