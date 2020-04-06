from kedro.pipeline import Pipeline, node

from fe_credit.nodes.primary_node.prm_data_node import *


def prm_data() -> Pipeline:
    """

    Returns: Pipeline to convert the raw data into intermediate data

    """
    return Pipeline([
        node(f_int_ccdm_cust_dtl, ['int_ccdm_cust_dtl'], 'prm_ccdm_cust_dtl', tags=['prm', 'ccdm_cust_dtl', 'prm_ccdm_cust_dtl']),
    ])



