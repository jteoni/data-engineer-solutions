def write_output(df, output_path):
    """
    Write PySpark DataFrame to CSV.

    Args:
    - df (DataFrame): PySpark DataFrame to write.
    - output_path (str): Path to write the CSV file.

    Returns:
    - None
    """
    df.coalesce(1).write.csv(output_path, mode="overwrite", header=True)
