import os
import uuid
import logging
import time

from datetime import datetime
from urllib3.util import Retry

from google.cloud import storage
from google.cloud.storage import Bucket

from google.cloud import bigquery
from google.cloud.bigquery import Table

def run():
    TIMEOUT = 10
    RETRY = Retry(backoff_factor=3)
    MAX_RETRIES = 10
    # Construct a BigQuery client object.
    client = bigquery.Client() #return QueryJob
    project = client.project
    query_str = """
        SELECT
        CONCAT(
            'https://stackoverflow.com/questions/',
            CAST(id as STRING)) as url,
        view_count
        FROM `bigquery-public-data.stackoverflow.posts_questions`
        WHERE tags like '%google-bigquery%'
        ORDER BY view_count DESC
        LIMIT 1"""
    sample_job_conf=bigquery.QueryJobConfig(
        use_legacy_sql = False,
        labels = {"name":"bq_example"},
        clustering = None
    )
    exec_date = datetime.utcnow()
    exec_date_fmt = "%Y_%m_%dT%H-%M-%S"
    exec_date_str = exec_date.strftime(exec_date_fmt)
    sample_job_id = f"big_query_sample_{exec_date_str}_{uuid.uuid4()}"
    job_location = "US"

    print('\t Query Job \t')
    query_job = client.query(
        location=job_location,
        query=query_str,
        job_config=sample_job_conf,
        job_id=sample_job_id
    )
    # results = query_job.result()  # Waits for job to complete.
    # print_rows(results)
    job = client.get_job(sample_job_id, location=job_location)  # API request
    # Print selected job properties
    print("Details for job {} running in {}:".format(sample_job_id, job_location))
    print("\tType: {}\n\tState: {}\n\tCreated: {}".format(job.job_type, job.state, job.created))

    dataset_id = "{}.bigquery_sample_dataset".format(client.project)
    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    try:
        # Send the dataset to the API for creation, with an explicit timeout.
        dataset = client.create_dataset(dataset, timeout=30, exists_ok=True)  # Make an API request.
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    except Exception as error:
        logging.error("Failed to create dataset {}".format(dataset_id))
        logging.error(error)
        pass

    #Listing datasets
    datasets = list(client.list_datasets(project))
    if datasets:
        print("Datasets in project : {}".format(project))
        for ds in datasets:
            print("\n - {}".format(ds.dataset_id))

    #Update dataset properties
    print("="*30,"Update dataset properties","="*30)
    my_dataset = client.get_dataset(dataset_id)
    my_dataset.description = "Update description at : {}".format(exec_date_str)
    client.update_dataset(my_dataset, ['description'])
    print("\n - {} \t Description: {}".format(my_dataset.dataset_id, my_dataset.description))

    print("="*30,"Table and Schema","="*30)
    table_name = "gsod_mini_10"
    table_id = "{}.{}".format(my_dataset.dataset_id, table_name)
    table_schema = [
        bigquery.SchemaField("station_number","INTEGER"),
        bigquery.SchemaField("year","INTEGER"),
        bigquery.SchemaField("month","INTEGER"),
        bigquery.SchemaField("day","INTEGER"),
        bigquery.SchemaField("max_temperature","FLOAT"),
        bigquery.SchemaField("min_temperature","FLOAT"),
        bigquery.SchemaField("rain","BOOLEAN"),
        bigquery.SchemaField("snow","BOOLEAN")
    ]

    # Insert row in table from Local Memory Data
    print("\t Insert row in table from : Local Memory Data \t")
    table_rows = [
        (1000,2020,12,25,float(18.30),float(5.50),True,False)
    ]
    table_rows_ids = range(len(table_rows))
    try:
        table_ref = my_dataset.table(table_name)
        table = Table(table_ref, table_schema)
        print('Table :  {}'.format(table))
        created_table_id = client.create_table(table, exists_ok=True)
        print("Created Table {}".format(created_table_id.table_id))
        num_rows = 0
        retries = 0
        while num_rows == 0 and retries < MAX_RETRIES:
            errors = client.insert_rows(table, table_rows, row_ids=table_rows_ids)
            print("Try to insert rows.\tErrors : {}".format(errors))
            query_job = client.query(query="SELECT * FROM {}.{}".format(project,table_id))
            results = query_job.result()
            num_rows = results.total_rows
            retries += 1
            time.sleep(3)
        print_rows(results)
    except Exception as error:
        logging.error("Can not load table data.")
        logging.error(error)

    # Insert row in table from Local File
    table_file = "samples/gsod_mini_10.json"

    # Insert row in table from URI
    table_load_job_id = f"big_query_sample_{exec_date_str}_{uuid.uuid4()}"
    table_uri = "gs://beam_sample_weather_dataset/gsod_mini_10"
    bucket_name = "beam_sample_weather_dataset"
    blob_name = "gsod_mini_10"
    storage_client = storage.client.Client()
    bucket = Bucket(storage_client,bucket_name)
    blob = bucket.blob(blob_name)
    print("\t Creating bucket : {} \t".format(bucket_name))
    try:
        storage_client.create_bucket(bucket, project)
        blob.upload_from_filename(table_file)
    except Exception as error:
        logging.error(error)

    print("\t Load table from : URI \t")
    table_load_job_conf = bigquery.LoadJobConfig(
        schema = table_schema,
        source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )
    load_job = client.load_table_from_uri(
        table_uri,
        table_id,
        job_id=table_load_job_id,
        location=job_location,
        job_config=table_load_job_conf
    )
    try:
        load_job.result(timeout=TIMEOUT)
        rows = client.list_rows(table, timeout=TIMEOUT)
        print_rows(rows)
    except Exception as error:
        logging.error(error)

    print("="*30,"Record & REPEATED Schema","="*30)
    table_schema.append(
        bigquery.SchemaField(
            "addresses", "RECORD",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField("ips", "STRING", mode="REPEATED"),
                bigquery.SchemaField("city","STRING"),
                bigquery.SchemaField("postal_code","INTEGER")
            ]
        )
    )
    table_with_repeated_field_ref = my_dataset.table("gsod_with_addresses")
    table_with_repeated_field = Table(table_with_repeated_field_ref, schema=table_schema)
    address_record = {"ips":["10.1.2.3.4","10.5.6.7.8"],"city":"NY", "postal_code":12345}
    table_with_repeated_field_rows = [ (1000,2020,12,25,float(18.30),float(5.50),True,False,address_record)]
    try:
        client.create_table(table_with_repeated_field, exists_ok=True)
        table = client.get_table(table_with_repeated_field_ref)
        client.insert_rows(table, table_with_repeated_field_rows)
        rows = client.list_rows(table)
        print_rows(rows)
    except Exception as error:
        logging.error(error)

    print("="*30,"Cleaning ...","="*30)
    #Cleaning
    client.delete_dataset(my_dataset.dataset_id, delete_contents=True, not_found_ok=True)
    print("Dataset {} deleted!".format(my_dataset.dataset_id))
    bucket = storage_client.get_bucket(bucket_name)
    bucket.delete(storage_client)
    print("Bucket {} deleted!".format(bucket.name))


def print_rows(results):
    result_list = list(results)
    print("Rows Count : {}".format(len(result_list)))
    for row in result_list:
        print(row)


if __name__ == "__main__":
    run()