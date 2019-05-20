from enum import Enum
from delta.etl.voyage_etl_delta_customer import VoyageEtlDeltaCustomer

class DagId(Enum):
    CCI_VAYOGE_TCUST_DELTA = "cci_voyage_tcust_delta"
    

class VoyageEtlFactory: 
    
    @staticmethod
    def factory(dagId , config):
        if DagId(dagId) is DagId.CCI_VAYOGE_TCUST_DELTA:
            return VoyageEtlDeltaCustomer(**config)
        else :
            raise ValueError("dagId implementation is not found")
            