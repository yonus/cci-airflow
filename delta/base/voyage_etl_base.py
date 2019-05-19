from abc import ABC , abstractmethod
from datetime import timedelta,datetime

from airflow import models
from airflow.utils.dates import days_ago
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from delta.utils.dags_utils_mssql_to_gcs_operator import MsSqlToGoogleCloudStorageOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook,BigQueryCursor
from airflow.operators.python_operator import PythonOperator

class VoyageEtlBase:
    LOG_TABLE_NAME = "voyage_etl_log"

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
        bigquery_write_disposition="WRITE_TRUNCATE",
        start_date=days_ago(0),
        schedule_interval=None):

        self.dag_id = dag_id
        self.project_id = project_id
        self.table_name = table_name
        self.gcs_bucket = gcs_bucket
        self.gbq_dataset = gbq_dataset
        self.query = query
        self.gbq_table = gbq_table
        self.mssql_conn_id = mssql_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.bigquery_conn_id = bigquery_conn_id
        self.bigquery_create_disposition = bigquery_create_disposition
        self.bigquery_write_disposition = bigquery_write_disposition
        self.start_date = start_date
        self.schedule_interval = schedule_interval

        self.__bigQueryHook = self.createBigQueryHook()
        
     
    def create_dag(self):
        
        dag = models.DAG(dag_id=self.dag_id, start_date=self.start_date, schedule_interval=self.schedule_interval,on_failure_callback=self.failLog)
        start_process_operator = PythonOperator(task_id='start_process_operator', python_callable=self.startProcessOperator, dag=dag)

        t1 = MsSqlToGoogleCloudStorageOperator(
            task_id="voyage_to_gcs",
            sql=self.getQuery(),
            bucket=self.gcs_bucket,
            filename="cci_voyage_%s_data.json" % self.table_name,
            schema_filename="cci_voyage_%s_schema.json" % self.table_name,
            mssql_conn_id=self.mssql_conn_id,
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            dag=dag
        )
            
        t2 = GoogleCloudStorageToBigQueryOperator(
            task_id="gcs_to_bq",
            bucket=self.gcs_bucket,
            source_objects=["cci_voyage_%s_data.json" % self.table_name] ,
            destination_project_dataset_table="%s.%s" % (self.gbq_dataset, self.gbq_table),
            source_format='NEWLINE_DELIMITED_JSON',
            schema_object="cci_voyage_%s_schema.json" % self.table_name,
            create_disposition=self.bigquery_create_disposition,
            write_disposition=self.bigquery_write_disposition,
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            bigquery_conn_id=self.bigquery_conn_id,
            dag=dag
        )

        success_log_operator = PythonOperator(task_id='success_log_operator', python_callable=self.successLogOperator, dag=dag)
        start_process_operator >> t1 >> t2 >> success_log_operator
        return dag
    
    
    def startProcessOperator(self,**kwargs):
        self.startProcessOperator()
        kwargs['ti'].xcom_push(key='start_time', value=datetime.now())

    def successLogOperator(self,**kwargs):
        ti = kwargs['ti']
        start_time = ti.xcom_pull(key=None, task_ids='start_process_operator')
        end_time = datetime.now()
        rows = []
        rows.append({"table_name":self.gbq_table,"start_time":start_time.isoformat(), "end_time":end_time.isoformat(),"status":"S","log_time":end_time.isoformat()})
        self.insertLog(rows)

    def failLog(self , **kwargs):
        ti = kwargs['ti']
        start_time = ti.xcom_pull(key=None, task_ids='start_process_operator')
        log_time = datetime.now()
        rows = []
        rows.append({"table_name":self.gbq_table,"start_time":start_time.isoformat() ,"status":"F" ,"log_time":log_time.isoformat()})
        self.insertLog(rows)


    def getBaseQuery(self):
        return self.query

    def getQuery(self):
        return self.getBaseQuery() + " " + self.getQueryCriteria()
        
    @abstractmethod
    def getQueryCriteria(self):
      pass

    def createBigQueryHook(self):
      bigQueryHook = BigQueryHook(self.bigquery_conn_id)
      return bigQueryHook
    
    def getCurrentBigQueryHook(self):
        return self.__bigQueryHook

    def createBigQueryCursor(self):
       connection = self.__bigQueryHook.get_conn()
       return connection.cursor()
     
    def createLogTableIfNotExist(self):
        isDeltaTableExist = self.__bigQueryHook.table_exists(self.project_id,self.gbq_dataset,self.LOG_TABLE_NAME)
        if isDeltaTableExist is False:
            cursor = self.createBigQueryCursor()
            schema_fields=[{"name": "table_name", "type": "STRING", "mode": "REQUIRED"},
                           {"name": "start_time", "type": "DATETIME", "mode": "NULLABLE"},
                           {"name": "end_time", "type": "DATETIME", "mode": "NULLABLE"},
                           {"name": "status", "type": "DATETIME", "mode": "REQUIRED"},
                           {"name": "log_time", "type": "DATETIME", "mode": "REQUIRED"}]

            cursor.create_empty_table(self.project_id,self.gbq_dataset,self.LOG_TABLE_NAME,schema_fields)

   
   
    def insertLog(self, rows):
        cursor = self.createBigQueryCursor()
        cursor.insert_all(self.project_id, self.gbq_dataset, self.LOG_TABLE_NAME,rows)
        
    
    def getLastSuccessfullEtlTime(self,etlTableName):
        query = "SELECT start_time FROM " + self.LOG_TABLE_NAME + " WHERE status = 'S' AND table_name ='" + etlTableName + "' "+ " ORDER BY start_time DESC LIMIT 1"
        bigQueryCursor = self.createBigQueryCursor()
        bigQueryCursor.execute(query);
        resultSet = bigQueryCursor.fetchone()
        if resultSet is None:
           return None
        else:
            print("last successfull etl time for table %s is %s " %(etlTableName,resultSet["start_time"]))
            return resultSet["start_time"]

    def postProcess(self,startDate, endDate, status):
        print("post process called")
        


        
