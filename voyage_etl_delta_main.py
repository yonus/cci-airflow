import yaml
from  delta.base.voyage_etl_factory import VoyageEtlFactory


etls = yaml.load(open("/home/airflow/gcs/dags/delta/config/voyage_etl_delta.yaml"))
for dag_config in etls["dags"]:
    dagId = dag_config["dag_id"]
    voyageEtlBase = VoyageEtlFactory.factory(dagId,dag_config)
    globals()[dagId] = voyageEtlBase.create_dag()
