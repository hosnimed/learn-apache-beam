import sys
import uuid
import logging
import time

from datetime import datetime
from urllib3.util import Retry

from google.cloud.exceptions import Conflict
from google.api_core.exceptions import NotFound

from google.cloud import storage
from google.cloud.storage import Bucket

from google.cloud import bigquery
from google.cloud.bigquery import Table


def run():
    TIMEOUT = 10
    RETRY = Retry(backoff_factor=3)
    MAX_RETRIES = 10
    try:
        # Construct a BigQuery client object.
        client = bigquery.Client()  # return QueryJob
        project = client.project
        exec_date = datetime.utcnow()
        exec_date_fmt = "%Y_%m_%dT%H-%M-%S"
        exec_date_str = exec_date.strftime(exec_date_fmt)
        sample_job_id = f"big_query_sample_{exec_date_str}_{uuid.uuid4()}"
        job_location = "US"

        query_str_sample = """
            SELECT
            CONCAT(
                'https://stackoverflow.com/questions/',
                CAST(id as STRING)) as url,
            view_count
            FROM `bigquery-public-data.stackoverflow.posts_questions`
            WHERE tags like '%google-bigquery%'
            ORDER BY view_count DESC
            LIMIT 1"""
        results = query_job_sample(client, job_location, sample_job_id, query_str_sample, query_priority="BATCH")
        print_rows(results)

        print_job_details(client, job_location, sample_job_id)

        dataset_id = create_dataset(client, timeout=TIMEOUT)

        # Listing datasets
        show_existing_datasets(client, project)

        # Update dataset properties
        print("=" * 30, "Update dataset properties", "=" * 30)
        description = "Update description @:{}".format(exec_date_str)
        my_dataset = update_dataset_description(client, dataset_id, description)

        print("=" * 30, "Table and Schema", "=" * 30)
        table_name = "gsod_mini_10"
        table_id = "{}.{}".format(my_dataset.dataset_id, table_name)
        table_schema = [
            bigquery.SchemaField("station_number", "INTEGER"),
            bigquery.SchemaField("year", "INTEGER"),
            bigquery.SchemaField("month", "INTEGER"),
            bigquery.SchemaField("day", "INTEGER"),
            bigquery.SchemaField("max_temperature", "FLOAT"),
            bigquery.SchemaField("min_temperature", "FLOAT"),
            bigquery.SchemaField("rain", "BOOLEAN"),
            bigquery.SchemaField("snow", "BOOLEAN")
        ]

        # Insert row in table from Local Memory Data
        print("\t Insert row in table from : Local Memory Sequence \t")
        table_rows = [
            (1000, 2020, 12, 25, float(18.30), float(5.50), True, False)
        ]
        table_rows_ids = range(len(table_rows))
        table_from_object = insert_rows_from_object(client, my_dataset, table_name, table_rows, table_schema)

        # Insert row in table from Local File
        table_file = "samples/gsod_mini_10.json"

        # Upload file to bucket
        bucket_name = "beam_sample_weather_dataset"
        blob_name = "gsod_mini_10"
        storage_client = storage.client.Client()
        create_bucket(storage_client, project, blob_name, bucket_name, table_file, exists_ok=False)

        # Insert row in table from URI
        table_load_job_id = f"big_query_sample_{exec_date_str}_{uuid.uuid4()}"
        table_uri = "gs://beam_sample_weather_dataset/gsod_mini_10"
        print("\t Load table from : URI \t")
        table_from_uri = insert_rows_from_uri(client, job_location, my_dataset, table_name, table_schema, table_uri, table_load_job_id,timeout=TIMEOUT)

        # Copy Table(s)
        print("=" * 30, "Copy Tables", "=" * 30)
        src_table1 = create_table(client, "src_table_1",table_schema, dataset=my_dataset)
        copy_table(client, table_from_object, src_table1)
        src_table2 = create_table(client, "src_table_2",table_schema, dataset=my_dataset)
        copy_table(client, table_from_object, src_table2)
        dst_table = create_table(client, "dst_table", table_schema, dataset=my_dataset)
        copy_table(client, [src_table1, src_table2], dst_table, delete_source=False)
        show_table_data(client,dst_table)

        print("=" * 30, "Record & REPEATED Schema", "=" * 30)
        table_schema.append(
            bigquery.SchemaField(
                "addresses", "RECORD",
                mode="NULLABLE",
                fields=[
                    bigquery.SchemaField("ips", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("city", "STRING"),
                    bigquery.SchemaField("postal_code", "INTEGER")
                ]
            )
        )
        table_name = "gsod_with_addresses"
        address_record = {"ips": ["10.1.2.3.4", "10.5.6.7.8"], "city": "NY", "postal_code": 12345}
        table_with_repeated_field_rows = [(1000, 2020, 12, 25, float(18.30), float(5.50), True, False, address_record)]
        # insert_rows_from_object(client, my_dataset, table_name, table_with_repeated_field_rows, table_schema, max_retries=MAX_RETRIES)

    except Exception as error:
        logging.error(error)
        raise error
    finally:
        # Cleaning
        print("=" * 30, "Cleaning ...", "=" * 30)
        client.delete_dataset(my_dataset.dataset_id, delete_contents=True, not_found_ok=True)
        print("Dataset {} deleted!".format(my_dataset.dataset_id))
        bucket = storage_client.get_bucket(bucket_name)
        bucket.delete(storage_client)
        print("Bucket {} deleted!".format(bucket.name))


def create_table(client, table_name, schema, project=None, dataset=None):
    """
    Create Table with Schema
    :param client: BQ Client
    :param table_name: Table name
    :param schema: Table Schema
    :param project: default to client.project
    :param dataset: default to client.dataset
    :return: created table
    """
    try:
        if project is None:
            project = client.project
        if dataset is None:
            dataset = client.dataset
        logging.info("Project: {}\tDataset: {}\tTable: {}".format(project, dataset.dataset_id, table_name))
        table_id = "{}.{}.{}".format(project, dataset.dataset_id, table_name)
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        table = client.get_table(table)
        logging.info("Table {} created successfully.".format(table_id))
        return table
    except Exception as error:
        raise error


def copy_table(client, source_tables, destination_table, delete_source=False, not_found_ok=True):
    """
    Copy single or multiple source table(s) to a destination table
    :param client: BQ Client
    :param source_tables: Single/List source tables
    :param destination_table: Destination table
    :param delete_source: Boolean, delete source table(s)
    :param not_found_ok: Boolean, raise exception if source table(s) not found
    :return: Creation/Deletion status
    """
    logging.info("Copying Table:\n From: {}\n To: {}".format(source_tables, destination_table))
    try:
        copy_job = client.copy_table(source_tables, destination_table)
        copy_result = copy_job.result()
        if copy_result:
            logging.info("Copy operation done with success")
            if delete_source:
                client.delete_table(source_tables)
    except NotFound as error:
        if not_found_ok:
            logging.info("Sources Table(s) deleted successfully.")
            return True
        else:
            logging.error("Delete operation failed: source table(s) not found.")
            logging.error(error)
            return False
    except Exception as error:
        logging.error("Can't copy table(s).")
        logging.error(error)
        return False
    else:
        return True


def insert_rows_from_uri(client, job_location, dataset, table_name, table_schema, table_uri, load_job_id, timeout=10):
    """
    Insert rows from remote
    :param client: BQ Client
    :param job_location : BQ job location
    :param dataset: BQ Dataset
    :param table_name: The table string name
    :param table_schema: The table schema
    :param table_uri: The GCS blob uri to insert
    :param load_job_id: Load job ID
    :param timeout : optional timeout, default to 10
    :return: The created Table
    """
    table_ref = dataset.table(table_name)
    table = Table(table_ref, table_schema)
    table_load_job_conf = bigquery.LoadJobConfig(
        schema=table_schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )
    load_job = client.load_table_from_uri(
        table_uri,
        table,
        job_id=load_job_id,
        location=job_location,
        job_config=table_load_job_conf
    )
    try:
        load_job.result(timeout=timeout)
        rows = client.list_rows(table, timeout=timeout)
        print_rows(rows)
    except Exception as error:
        logging.error(error)
    else:
        return table


def create_bucket(storage_client, project, blob_name, bucket_name, table_file, exists_ok=True):
    """
    Create a GCS Bucket
    :param storage_client: The GCS client
    :param project: The GCS project
    :param blob_name: The GCS blob/object name
    :param bucket_name: The bucket name
    :param table_file: Local file to upload to bucket
    :param exists_ok: Boolean, if `True` ignore "already exists"
    :return: The creation status : False if already exist and exists_ok set to False
    """
    bucket = Bucket(storage_client, bucket_name)
    blob = bucket.blob(blob_name)
    try:
        bucket_exist = storage_client.lookup_bucket(bucket_name)
        if bucket_exist:
            if exists_ok:
                logging.info("Bucket {} already exist.".format(bucket_name))
                return True
            else:
                logging.error("Bucket {} already exist.".format(bucket_name))
                return False
        else:
            print("\t Creating bucket : {} \t".format(bucket_name))
            storage_client.create_bucket(bucket, project)
            blob.upload_from_filename(table_file)
            return True
    except Conflict:
        if exists_ok:
            return True
        else:
            return False
    except Exception as error:
        logging.error(error)
        raise


def insert_rows_from_object(client, dataset, table_name, table_rows, table_schema, table_rows_ids=None, max_retries=3):
    """
    Insert rows to a table from a sequence
    :param client: BQ Client
    :param dataset: BQ Dataset
    :param table_name: The table string name
    :param table_rows: The sequence data
    :param table_schema: The table schema
    :param table_rows_ids: The table rows ids, default ot : range(len(table_row))
    :param max_retries: Max retries, default to 3
    :return: The created Table
    """
    try:
        print("Insert {} rows to Table: {}.{}".format(len(table_rows), dataset.dataset_id, table_name))
        table = create_table(client, table_name, table_schema, dataset=dataset)
        full_table_id = table.full_table_id \
            if table.full_table_id is not None \
            else "{}.{}.{}".format(client.project, dataset, table_name)
        full_table_id = str.replace(full_table_id, ":", ".")
        logging.info("FULL_TABLE_ID :: {}".format(full_table_id))
        table_rows_ids = range(len(table_rows)) if table_rows_ids is None else table_rows_ids
        num_rows = 0
        retries = 0
        while num_rows == 0 and retries < max_retries:
            errors = client.insert_rows(table, table_rows, row_ids=table_rows_ids)
            print("Try to insert rows.\tErrors : {}".format(errors))
            query_job = client.query(query="SELECT * FROM {}".format(full_table_id))
            results = query_job.result()
            num_rows = results.total_rows
            retries += 1
            time.sleep(3)
        return table
    except Exception as error:
        logging.error("Can not load table data.")
        logging.error(error)
        raise


def update_dataset_description(client, dataset_id, description=None):
    """
    Update dataset description
    :param client: BQ Client
    :param dataset_id: The Dataset ID
    :param description: The description
    :return: The dataset updated
    """
    my_dataset = client.get_dataset(dataset_id)
    my_dataset.description = description
    client.update_dataset(my_dataset, ['description'])
    print("\n - {} \t Description: {}".format(my_dataset.dataset_id, my_dataset.description))
    return my_dataset


def show_existing_datasets(client, project):
    """
    Show existing datasets in a project
    :param client: BQClient
    :param project: Project ID to use for retreiving datasets
    :return: None
    """
    datasets = list(client.list_datasets(project))
    if datasets:
        print("Datasets in project : {}".format(project))
        for ds in datasets:
            print("\n - {}".format(ds.dataset_id))


def create_dataset(client, timeout=30):
    """
    Create a dataset in the client default project
    :param client: BQ Client
    :param timeout: TIMEOUT
    :return: Datset ID
    """
    dataset_id = "{}.bigquery_sample_dataset".format(client.project)
    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    try:
        # Send the dataset to the API for creation, with an explicit timeout.
        dataset = client.create_dataset(dataset, timeout=timeout, exists_ok=True)  # Make an API request.
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    except Exception as error:
        logging.error("Failed to create dataset {}".format(dataset_id))
        logging.error(error)
        raise
    return dataset_id


def print_job_details(client, job_location, job_id):
    """
    Print BQ Job details : Type - State - Created
    :param client: BQ Client
    :param job_location: Job location
    :param job_id: Job ID
    :return: None
    """
    job = client.get_job(job_id, location=job_location)  # API request
    # Print selected job properties
    print("Details for job {} running in {}:".format(job_id, job_location))
    print("\tType: {}\n\tState: {}\n\tCreated: {}".format(job.job_type, job.state, job.created))


def query_job_sample(client, job_location, job_id, query_str, query_priority="interactive"):
    """
    Create and execute a query job
    :param client: BQ Client
    :param job_location: Job location
    :param job_id: Job ID
    :param query_str: The Query string
    :param query_priority: Interactive by default or Batch
    :return: result job rows iterator
    """
    print('\t Query Job \t')
    priorities = {"interactive": bigquery.QueryPriority.INTERACTIVE, "batch": bigquery.QueryPriority.BATCH}
    sample_job_conf = bigquery.QueryJobConfig(
        use_legacy_sql=False,
        labels={"name": "bq_example"},
        priority = priorities.get(query_priority.lower()),
        clustering=None
    )
    query_job = client.query(
        location=job_location,
        query=query_str,
        job_config=sample_job_conf,
        job_id=job_id
    )
    results = query_job.result()  # Waits for job to complete.
    return results


def print_rows(results):
    result_list = list(results)
    print("Rows Count : {}".format(len(result_list)))
    for row in result_list:
        print(row)


def show_table_data(client,table,max_result=10):
    # Print row data in tabular format.
    rows = client.list_rows(table, max_results=max_result)
    format_string = "{!s:<16} " * len(rows.schema)
    field_names = [field.name for field in rows.schema]
    print(format_string.format(*field_names))  # Prints column headers.
    for row in rows:
        print(format_string.format(*row))  # Prints row data.

if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
    run()
