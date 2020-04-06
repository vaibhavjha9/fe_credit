from fe_credit.nodes.utilities.dq_functions_nodes import *
from pyspark.sql import functions as F


def f_int_ccdm_cust_dtl(int_ccdm_cust_dtl):
    """
    The function converts the intermediate ccdm_cust_dtl data into intermediate version
    Args:
        int_ccdm_cust_dtl
    Returns:
        prm_ccdm_cust_dtl
    """

    prm_ccdm_cust_dtl = int_ccdm_cust_dtl
    return prm_ccdm_cust_dtl
