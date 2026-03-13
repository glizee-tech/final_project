from pyspark.sql import functions as F
from pyspark.sql import Window

def clean_integer_column(df, col_name, to_delete=False, accept_float=False):
    """
    Clean a column expected to contain integers.

    Behaviors:
    - If accept_float=False:
        Accept only:
        - 123
        - 123.0
        - 123.00
        Invalid values become null.
    - If accept_float=True:
        Accept any numeric value:
        - 123
        - 123.0
        - 123.45
        - 123.99
        Values are rounded to nearest integer.

    Parameters:
        df : Spark DataFrame
        col_name : str
            Column to clean
        to_delete : bool
            - False: keep invalid values as null in the cleaned column
            - True: remove rows where cleaned value is null and return them in quarantine
        accept_float : bool
            - False: only accept integers or floats ending in .0 / .00 / ...
            - True: accept any numeric float and round to integer

    Returns:
        If to_delete=False:
            df_cleaned

        If to_delete=True:
            df_valid, df_quarantine
    """

    col_str = F.trim(F.col(col_name).cast("string"))

    if accept_float:
        # Accept any numeric value (integer or float), then round to integer
        cleaned_col = F.when(
            col_str.rlike(r"^[0-9]+(\.[0-9]+)?$"),
            F.round(col_str.cast("double")).cast("int")
        ).otherwise(None)

    else:
        # Accept only integers or values ending with .0 / .00 / ...
        clean_str = F.regexp_replace(col_str, r"\.0+$", "")
        cleaned_col = F.when(
            col_str.rlike(r"^[0-9]+(\.0+)?$"),
            clean_str.cast("int")
        ).otherwise(None)

    # Add cleaned version temporarily
    df_with_clean = df.withColumn("__cleaned_col__", cleaned_col)

    if not to_delete:
        # Replace original column with cleaned column
        df_cleaned = (
            df_with_clean
            .drop(col_name)
            .withColumnRenamed("__cleaned_col__", col_name)
        )
        return df_cleaned

    # Split valid / quarantine
    df_valid = (
        df_with_clean
        .filter(F.col("__cleaned_col__").isNotNull())
        .drop(col_name)
        .withColumnRenamed("__cleaned_col__", col_name)
    )

    df_quarantine = (
        df_with_clean
        .filter(F.col("__cleaned_col__").isNull())
        .drop("__cleaned_col__")
        .withColumn(
            "quarantine_reason",
            F.when(F.col(col_name).isNull(), F.lit(f"missing_{col_name}"))
             .otherwise(F.lit(f"invalid_{col_name}"))
        )
        .withColumn("quarantine_timestamp", F.current_timestamp())
    )

    return df_valid, df_quarantine

def parse_date_col(col_name):
    """
    Parse a Spark column containing dates stored as strings in multiple possible formats.

    This function trims whitespace, casts the column to string, and attempts to parse
    the value using a list of supported date and datetime formats. It returns the first
    successfully parsed value as a DATE type using `coalesce`.

    Supported formats include:
    - ISO date formats with dashes
    - ISO-like date formats with slashes
    - European date formats with dashes
    - Datetime formats where time is ignored and only the date is kept

    Parameters
    ----------
    col_name : str
        Name of the Spark DataFrame column to parse.

    Returns
    -------
    pyspark.sql.column.Column
        A Spark Column of type DATE if parsing succeeds, otherwise NULL.

    Notes
    -----
    - Invalid or unrecognized date strings return NULL.
    - The function uses `try_to_timestamp`, so malformed values do not raise errors.
    - Time information is discarded by converting parsed timestamps to DATE.
    """
    c = F.trim(F.col(col_name).cast("string"))

    date_formats = [
        # ISO date formats with dashes
        "yyyy-MM-dd",
        "yyyy-M-d",
        "yyyy-M-dd",
        "yyyy-MM-d",

        # ISO-like date formats with slashes
        "yyyy/MM/dd",
        "yyyy/M/d",
        "yyyy/M/dd",
        "yyyy/MM/d",

        # European date formats with dashes
        "dd-MM-yyyy",
        "d-M-yyyy",
        "d-MM-yyyy",
        "dd-M-yyyy",

        # Datetime formats (time is ignored)
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-M-d'T'HH:mm:ss",
        "yyyy-MM-d'T'HH:mm:ss",
        "yyyy-M-dd'T'HH:mm:ss",
    ]

    parsed_dates = [
        F.to_date(F.try_to_timestamp(c, F.lit(fmt)))
        for fmt in date_formats
    ]

    return F.coalesce(*parsed_dates)


def quarantine_full_duplicates(df, quarantine_reason="full_duplicated_line"):
    """
    Remove fully duplicated rows while keeping the first occurrence.
    Send removed duplicate rows to quarantine.

    Parameters:
        df : Spark DataFrame
        quarantine_reason : str, reason added to quarantined rows

    Returns:
        df_without_duplicates : DataFrame with only first occurrences kept
        df_quarantine : DataFrame containing removed full duplicates with quarantine metadata
    """

    window_all = Window.partitionBy(df.columns).orderBy(F.lit(1))

    df_with_rownum = df.withColumn("row_num", F.row_number().over(window_all))

    df_without_duplicates = (
        df_with_rownum
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    df_quarantine = (
        df_with_rownum
        .filter(F.col("row_num") > 1)
        .drop("row_num")
        .withColumn("quarantine_reason", F.lit(quarantine_reason))
        .withColumn("quarantine_timestamp", F.current_timestamp())
    )

    return df_without_duplicates, df_quarantine


def validate_date_order(df, date_late_col, date_early_col, primary_key):
    """
    Filter DataFrame rows where date_late_col >= date_early_col (if neither is null).
    Rows failing this condition are moved to a quarantine DataFrame with reason and timestamp.

    Parameters:
        df : Spark DataFrame
        date_late_col : str, the column that should be later (delivery_date_expected)
        date_early_col : str, the column that should be earlier (order_date)

    Returns:
        df_valid : DataFrame with valid rows
        df_quarantine : DataFrame with invalid rows and quarantine columns
    """

    # Step 1: valid rows
    df_valid = df.filter(
        (F.col(date_late_col).isNull()) | 
        (F.col(date_early_col).isNull()) |
        (F.col(date_late_col) >= F.col(date_early_col))
    )

    # Step 2: rows failing the condition
    df_quarantine = df.join(
        df_valid.select(primary_key),  # assuming order_id is unique
        on=primary_key,
        how="left_anti"
    ).withColumn(
        "quarantine_reason",
        F.lit(f"{date_late_col}_before_{date_early_col}")
    ).withColumn(
        "quarantine_timestamp",
        F.current_timestamp()
    )

    return df_valid, df_quarantine


def validate_foreign_key(df_main, df_reference, fk_col, pk_col, primary_key_main):
    """
    Validate a foreign key column in a main DataFrame against a reference DataFrame.
    Rows with invalid foreign keys are moved to a quarantine DataFrame with reason and timestamp.

    Parameters:
        df_main : Spark DataFrame containing the main table
        df_reference : Spark DataFrame containing the reference table (primary keys)
        fk_col : str, column name in df_main that is the foreign key
        pk_col : str, column name in df_reference that is the primary key
        primary_key_main : str, column name of the primary key in df_main (unique per row)

    Returns:
        df_valid : rows in df_main with fk_col valid
        df_quarantine : rows in df_main with invalid fk_col, plus quarantine_reason and quarantine_timestamp
    """

    # Step 1: keep only rows with valid foreign keys
    df_valid = df_main.join(
        df_reference.select(pk_col).distinct().withColumnRenamed(pk_col, fk_col),
        on=fk_col,
        how="inner"
    )

    # Step 2: rows with invalid foreign keys
    df_quarantine = df_main.join(
        df_valid.select(primary_key_main),
        on=primary_key_main,
        how="left_anti"
    ).withColumn(
        "quarantine_reason",
        F.lit(f"{fk_col}_not_in_reference")
    ).withColumn(
        "quarantine_timestamp",
        F.current_timestamp()
    )

    return df_valid, df_quarantine


def missing_values_report(df):
    """
    Return a long-format DataFrame with missing value counts per column.

    Missing values include:
    - true null
    - empty string ""
    - string values: "nan", "none", "null" (case insensitive)

    Parameters:
        df : Spark DataFrame

    Returns:
        df_missing : Spark DataFrame with columns:
            - column_name
            - missing_count
    """

    # Build one small DataFrame per column, then union all
    dfs = []

    for c in df.columns:
        df_col = df.select(
            F.lit(c).alias("column_name"),
            F.sum(
                F.when(
                    F.col(c).isNull() |
                    (F.trim(F.col(c).cast("string")) == "") |
                    (F.lower(F.trim(F.col(c).cast("string"))).isin("nan", "none", "null")),
                    1
                ).otherwise(0)
            ).alias("missing_count")
        )
        dfs.append(df_col)

    # Union all per-column results
    df_missing = dfs[0]
    for d in dfs[1:]:
        df_missing = df_missing.unionByName(d)

    return df_missing
