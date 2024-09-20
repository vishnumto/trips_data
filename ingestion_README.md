# trips_data    //
 Import necessary libraries
    IMPORT pyspark.sql.functions as F
    IMPORT pyspark.sql.types as T
    IMPORT delta.tables

    // Set up secret scope for Azure credentials
    SET SECRET_SCOPE = "AllUsers_SecretScope"
    RETRIEVE ADLS_NAME, ADLS_FILE_SYSTEM, ADLS_ACCOUNT_KEY from secret scope

    // Configure Spark to access ADLS
    SET Azure configuration using ADLS_NAME and ADLS_ACCOUNT_KEY

    // Load required libraries for data processing
    IMPORT json
    IMPORT pandas as pd
    IMPORT BlobServiceClient from azure.storage.blob

    // Initialize Blob Service Client to read JSON from Azure Blob Storage
    INITIALIZE Blob Service Client with credentials

    // Specify the container and blob (file) path
    DEFINE container_name and blob_name for JSON file

    // Read the blob into a string
    DOWNLOAD blob content
    PARSE JSON data into a Python object

    // Convert JSON data to a Pandas DataFrame
    CREATE Pandas DataFrame from parsed JSON data

    // Display the first few rows of the Pandas DataFrame
    PRINT first few rows of df_pandas

    // Convert Pandas DataFrame to Spark DataFrame
    CREATE Spark DataFrame from df_pandas

    // Display the Spark DataFrame
    DISPLAY df_spark

    // Identify columns ending with 'Duration'
    CREATE list of duration_columns from df_spark

    // Function to convert HH:MM:SS to seconds for specified columns in a DataFrame
    DEFINE function apply_duration_conversion(df, duration_columns):
        FOR each col_name in duration_columns:
            CONVERT col_name from HH:MM:SS to seconds
        RETURN updated DataFrame

    // Apply the conversion to the duration columns
    df_spark = apply_duration_conversion(df_spark, duration_columns)

    // Display the updated DataFrame
    DISPLAY df_spark

    // Define the schema for the DataFrame with comments
    DEFINE schema with field types and metadata

    // Create a new DataFrame with the defined schema
    df_transformed = CREATE new DataFrame using df_spark.rdd and schema

    // Display the transformed DataFrame
    DISPLAY df_transformed
