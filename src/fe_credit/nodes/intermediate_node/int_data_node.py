from fe_credit.nodes.utilities.dq_functions_nodes import *
from pyspark.sql import functions as F


def f_int_ccdm_cust_dtl(raw_ccdm_cust_dtl):
    """
    The function converts the raw ccdm_cust_dtl data into intermediate version
    Args:
        raw_ccdm_cust_dtl
    Returns:
        intermediate_ccdm_cust_dtl
    """

    int_ccdm_cust_dtl = raw_ccdm_cust_dtl
    return clean_col_names_for_pandas(int_ccdm_cust_dtl)
