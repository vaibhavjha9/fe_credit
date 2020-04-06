from kedro.pipeline import Pipeline, node

from fe_credit.nodes.intermediate_node.int_data_node import *


def int_data() -> Pipeline:
    """

    Returns: Pipeline to convert the raw data into intermediate data

    """
    return Pipeline([
        node(f_int_ccdm_cust_dtl, ['raw_ccdm_cust_dtl'], 'int_ccdm_cust_dtl', tags=['int', 'ccdm_cust_dtl', 'int_ccdm_cust_dtl']),
    ])



