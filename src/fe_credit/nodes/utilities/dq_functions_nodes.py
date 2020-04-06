import logging
import re
from functools import reduce
import pandas as pd
import pyspark
from pyspark.sql import DataFrame, functions as F
from pyspark.sql import SparkSession


def get_spark():
    spark = SparkSession.builder.getOrCreate()
    return spark


def clean_col_names(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    1. Replaces spaces in pandas columns with underscores.
    2. Lower cases column names
    :param input_df: pd.DataFrame
    :return: pd.DataFrame
    """

    return input_df.rename(columns=lambda name: name.replace(' ', '_').lower())


def clean_col_names_for_parquet(input_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Parquet will not support spaces or certain other characters, this node substitutes all for underscores.
    Args:
        input_df: pyspark.sql.DataFrame
    Returns: pyspark.sql.DataFrame
    """

    def clean_spark_col_name(column_name):
        column = column_name
        column = str.replace(column, '+', '_')  # Replaces + with _
        column = re.sub('([A-Z]+)', r'_\1', column)  # replaces ShiftNo with shift_no and RLN to rln
        column = re.sub(r'[-",;.\{\}\(\)\n\t=\s/]', '_', column)  # adds _ in column names
        column = re.sub(r'[#]', '_num_', column)  # adds _ in column names
        column = re.sub(r'[%]', '_perc_', column)  # replaces % with _perc_
        column = re.sub(r'[\?]', '', column)  # removes ? in the column names
        column = re.sub(r'(^_)', '', column)  # removes _ at the beginning if added during above steps
        column = re.sub(r'(__*)', '_', column)  # replaces subsequent _'s with single _
        column = re.sub(r'(_$)', '', column)  # removes _ at the end of column name
        column = re.sub(r'(^_)', '', column)  # removes _ at the beginning if added during above steps
        column = re.sub(r'(_v$)', '', column)  # removes _v at the end of column name
        column = re.sub(r'(_x$)', '', column)  # removes _x at the end of column name
        column = re.sub(r'(_q$)', '', column)  # removes _q at the end of column name
        column = re.sub(r'(_y$)', '', column)  # removes _y at the end of column name

        return column.lower()

    column_remapping = [(original_column, clean_spark_col_name(original_column)) for original_column in
                        input_df.columns]
    output_df = input_df
    for original_column_name, new_column_name in column_remapping:
        output_df = output_df.withColumnRenamed(original_column_name, new_column_name)

    logging.info(
        'column remappings: {}'.format(
            '|'.join(['"{}"->"{}"'.format(init, repl) for init, repl in zip(input_df.columns, output_df.columns)])
        ))
    return output_df


def clean_col_names_for_pandas(input_df: pd.DataFrame) -> pyspark.sql.DataFrame:
    """
    Pandas Data Frame Cleaning Names
    Args:
        input_df: pd.DataFrame
    Returns: pyspark.sql.DataFrame
    """

    def clean_pandas_col_name(column_name):
        column = column_name
        column = str.replace(column, '+', '_')  # Replaces + with _
        column = re.sub('([A-Z]+)', r'_\1', column)  # replaces ShiftNo with shift_no and RLN to rln
        column = re.sub(r'[-",;.\{\}\(\)\n\t=\s/]', '_', column)  # adds _ in column names
        column = re.sub(r'[#]', '_num_', column)  # adds _ in column names
        column = re.sub(r'[%]', '_perc_', column)  # replaces % with _perc_
        column = re.sub(r'[\?]', '', column)  # removes ? in the column names
        column = re.sub(r'(^_)', '', column)  # removes _ at the beginning if added during above steps
        column = re.sub(r'(__*)', '_', column)  # replaces subsequent _'s with single _
        column = re.sub(r'(_$)', '', column)  # removes _ at the end of column name
        column = re.sub(r'(^_)', '', column)  # removes _ at the beginning if added during above steps
        return column.lower()

    column_remapping = [(original_column, clean_pandas_col_name(original_column)) for original_column in
                        input_df.columns]
    output_df = input_df
    for original_column_name, new_column_name in column_remapping:
        output_df.rename(columns={original_column_name: new_column_name}, inplace=True)

    logging.info(
        'column remappings: {}'.format(
            '|'.join(['"{}"->"{}"'.format(init, repl) for init, repl in zip(input_df.columns, output_df.columns)])
        ))
    return output_df


def distinct_rows(input_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    distinct rows
    Args:
        input_df: pyspark.sql.DataFrame
    Returns: pyspark.sql.DataFrame
    """
    output_df = input_df.distinct()
    input_rc = input_df.count()
    output_rc = output_df.count()
    if input_rc != output_rc:
        logging.info('Rows eliminated by distinct operation = {}'.format(input_rc - output_rc))
    return output_df


def union_dataframes_with_missing_cols(df_input_or_list, *args):
    if type(df_input_or_list) is list:
        df_list = df_input_or_list
    elif type(df_input_or_list) is DataFrame:
        df_list = [df_input_or_list] + list(args)

    col_list = set()
    for df in df_list:
        for column in df.columns:
            col_list.add(column)

    def add_missing_cols(dataframe, col_list):
        missing_cols = [column for column in col_list if column not in dataframe.columns]
        for column in missing_cols:
            dataframe = dataframe.withColumn(column, F.lit(None))
        return dataframe.select(*sorted(col_list))

    df_list_updated = [add_missing_cols(df, col_list) for df in df_list]
    return reduce(DataFrame.union, df_list_updated)


def pyspark_to_pandas(data_frame: DataFrame) -> pd.DataFrame:
    """
    Args:
        data_frame:

    Returns:

    """

    return data_frame.toPandas()


def pandas_to_pyspark(data_frame: pd.DataFrame, outputSchema) -> DataFrame:
    """
        Args:
            data_frame: Pandas DataFrame

        Returns:
            Spark Dataframe
    """
    sdf = get_spark().createDataFrame(data_frame, schema=outputSchema)
    return sdf

