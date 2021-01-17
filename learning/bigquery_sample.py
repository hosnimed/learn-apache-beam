import sys
import os
import uuid
import logging
import time
import copy
import random

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from dateutil import tz
from pytz import timezone, all_timezones_set
from urllib3.util import Retry
from waiting import wait, TimeoutExpired

from google.cloud.exceptions import Conflict
from google.api_core.exceptions import NotFound
from google.api_core.exceptions import GoogleAPICallError

from google.cloud import storage
from google.cloud.storage import Bucket

from google.cloud import bigquery
from google.cloud.bigquery import QueryJob
from google.cloud.bigquery import Table
from google.cloud.bigquery import TimePartitioning
from google.cloud.bigquery import TimePartitioningType
from google.cloud.bigquery import RangePartitioning
from google.cloud.bigquery import PartitionRange

@dataclass
class PartitionTimeFilter:
    exact_time : datetime    = None
    lower_bound_datetime : datetime = None
    upper_bound_datetime : datetime = None

TIME_ZONE = "Europe/Paris"
TIME_ZONE_FMT = "%Y-%m-%d %H:%M:%S %Z%z"
TIMEOUT = 10
RETRY = Retry(backoff_factor=3)
MAX_RETRIES = 10

def run():
    CLEANING = True
    LOCATION = "US"
    try:
        env = os.getenv("BQ_CLEANING")
        if env is not None:
            CLEANING = True if env.lower() == "true" else False
    except KeyError:
        logging.error("BQ_CLEANING system env not found.")
        pass
    except:
        raise
    print(f"BQ_CLEANING : {CLEANING}")
    try:
        env = os.getenv("BQ_LOCATION")
        if env is not None:
            LOCATION = env.upper()
    except KeyError:
        logging.error("BQ_LOCATION system env not found.")
    except:
        raise
    print(f"BQ_LOCATION : {LOCATION}")

    # Construct a BigQuery client object.
    client = bigquery.Client()
    storage_client = storage.client.Client()
    project = client.project

    my_dataset_id = "my_dataset"
    bucket_name = "beam_sample_weather_dataset"

    exec_date = datetime.utcnow()
    exec_date_fmt = "%Y_%m_%dT%H-%M-%S"
    exec_date_str = exec_date.strftime(exec_date_fmt)
    sample_job_id = f"big_query_sample_{exec_date_str}_{uuid.uuid4()}"
    job_location = LOCATION

    # Create Dataset
    print("=" * 50, "Create Dataset", "=" * 50)
    my_dataset = create_dataset(client, my_dataset_id, job_location, project, timeout=TIMEOUT)

    try:
        # Listing datasets
        # show_existing_datasets(client, project)
        # Update my_dataset properties
        # print("=" * 50, "Update my_dataset properties", "=" * 50)
        # description = "Update description @:{}".format(exec_date_str)
        # my_dataset = update_dataset_description(client, my_dataset_id, description)

        print("=" * 50, "Query Job sample", "=" * 50)
        query_str_sample = """
            SELECT
            CONCAT(
                'https://stackoverflow.com/questions/',
                CAST(id as STRING)) as url,
            view_count
            FROM `bigquery-public-data.stackoverflow.posts_questions`
            WHERE tags like '%@tag%'
            ORDER BY view_count DESC
            LIMIT 1"""
        query_params_sample = [
            bigquery.ScalarQueryParameter("tag","STRING","google-bigquery") #named param
            # bigquery.ScalarQueryParameter(None,"STRING","google-bigquery") #postional param
        ]
        dest_table_id_sample = "{}.{}.{}".format(project,my_dataset_id,"sample_query_table_id")
        # results = query_job_sample(client, job_location, sample_job_id, query_str_sample, query_params=query_params_sample, dest_table_id=dest_table_id_sample, query_priority="interactive")
        # print_rows(results)
        # show_table_data(client,dest_table_id_sample)
        # print_job_details(client, job_location, sample_job_id)

        print("=" * 50, "Table and Schema", "=" * 50)
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
        table_from_object = insert_rows_from_object(client, my_dataset, table_name, table_rows, table_schema, table_rows_ids=table_rows_ids)
        show_table_data(client, table_from_object)

        # Insert row in table from Local File
        table_file = "samples/gsod_mini_10.json"

        # Upload file to bucket
        blob_name = "gsod_mini_10"
        create_bucket(storage_client, project, blob_name, bucket_name, table_file, exists_ok=False)

        # Insert row in table from URI
        table_load_job_id = f"big_query_sample_{exec_date_str}_{uuid.uuid4()}"
        table_uri = "gs://beam_sample_weather_dataset/gsod_mini_10"
        print("\t Load table from : URI \t")
        # table_from_uri = insert_rows_from_uri(client, job_location, my_dataset, table_name, table_schema, table_uri,table_load_job_id, timeout=TIMEOUT)

        print("=" * 50, "Table Partitioning", "=" * 50)
        table_partition_insert_job_id = f"big_query_sample_{exec_date_str}_{uuid.uuid4()}"
        table_partitioned_schema = [
            bigquery.SchemaField("name","STRING"),
            bigquery.SchemaField("age","INTEGER"),
            bigquery.SchemaField("dob","DATE")
        ]
        print("\tTime Partitioned\t")
        table_time_partitioned_name = "my_table_time_partitioned"
        table_time_partitioned = create_table(client, table_time_partitioned_name, table_partitioned_schema, project, my_dataset, partitioning_type="time", partitioned_field=None)
        utc_time_now = datetime.now(timezone('UTC'))
        utc_time_plus_1_hour = utc_time_now + timedelta(hours=1)
        age_1 = 30
        age_2 = 50
        table_time_partitioned_data = (
            (f"John", age_1, date.today().replace(date.today().year-age_1, 4, 1).strftime("%Y-%m-%d")),
            (f"Jake", age_2, date.today().replace(date.today().year-age_2, 4, 1).strftime("%Y-%m-%d")),
        )
            # [{"name":f"Person-{uuid.uuid4()}", "age":int(30*random.random()), "dob":date.today().replace(1990,4,1)        }]
        insert_rows_from_object_partition_cluster_aware(client, job_location, table_partition_insert_job_id, my_dataset, table_time_partitioned_name, table_time_partitioned_data, table_partitioned_schema, partitioning_type="time", create_table_bool=False)
        show_table_data(client,table_time_partitioned)
        # Query partitioned table
        partition_job_id = f"big_query_sample_{exec_date_str}_{uuid.uuid4()}"
        partition_query_str = """
        SELECT _PARTITIONTIME AS pt,
        name, age, dob
        FROM {}.{}.{}
        """.format(project,my_dataset_id,table_time_partitioned_name)
        utc_exact_date = utc_time_now.replace(minute=0,second=0,microsecond=0)
        utc_lower_bound = utc_time_now.replace(minute=0,second=0,microsecond=0)
        utc_upper_bound = utc_time_plus_1_hour.replace(minute=0,second=0,microsecond=0)
        local_exact_date = utc_to_local(utc_exact_date)
        local_lower_bound = utc_to_local(utc_lower_bound)
        local_upper_bound = utc_to_local(utc_upper_bound)
        print("+"*100)
        print(local_exact_date.strftime(TIME_ZONE_FMT))
        print(local_lower_bound.strftime(TIME_ZONE_FMT))
        print(local_upper_bound.strftime(TIME_ZONE_FMT))
        print("+"*100)
        # results = query_job_partition_aware(client, job_location, partition_job_id, partition_query_str,
        #                                     partition_time_filter=PartitionTimeFilter(lower_bound_datetime=utc_lower_bound),
        #                                     zone_partition_time_filter=PartitionTimeFilter(exact_time=None,lower_bound_datetime=local_lower_bound,upper_bound_datetime=local_upper_bound),
        #                                     use_partition_time_filter=True,
        #                                     use_zone_partition_time_filter=True)
        # print_rows(results)
        print("\tRange Partitioned\t")
        # table_range_partitioned_name = "my_table_range_partitioned"
        # table_range_partitioned = create_table(client, table_range_partitioned_name, table_partitioned_schema, project,my_dataset, partitioning_type="range", partitioned_field="age")
        # show_table_data(client, table_range_partitioned)

        print("=" * 50, "Table Clustering", "=" * 50)
        # Clustering
        cluster_job_id = f"big_query_sample_{exec_date_str}_{uuid.uuid4()}"
        clustered_table_name = "my_clustered_table"
        clustered_table_schema = [
            bigquery.SchemaField("name","STRING"),
            bigquery.SchemaField("age","INTEGER"),
            bigquery.SchemaField("dob","DATE")
        ]
        clustered_table_rows = (
            (f"Person_Cluster_001", age_1, date.today().replace(date.today().year-age_1, 1, 1).strftime("%Y-%m-%d")),
            (f"Person_Cluster_002", age_2, date.today().replace(date.today().year-age_2, 1, 1).strftime("%Y-%m-%d")),
        )
        clustering_fields = ["age", "dob"]
        create_table(client, clustered_table_name, clustered_table_schema,project, my_dataset, clustering_fields=clustering_fields, partitioning_type=None)
        insert_rows_from_object_partition_cluster_aware(client, job_location, cluster_job_id, my_dataset, clustered_table_name, clustered_table_rows, clustered_table_schema, partitioning_type=None, clustering_fields=clustering_fields, create_table_bool=False)
        show_table_data(client, "{}.{}.{}".format(project,my_dataset.dataset_id,clustered_table_name))

        # Copy Table(s)
        print("=" * 50, "Copy Tables", "=" * 50)
        # src_table1 = create_table(client, "src_table_1", table_schema, dataset=my_dataset)
        # copy_table(client, table_from_object, src_table1)
        # src_table2 = create_table(client, "src_table_2", table_schema, dataset=my_dataset)
        # copy_table(client, table_from_object, src_table2)
        # dst_table = create_table(client, "dst_table", table_schema, dataset=my_dataset)
        # copy_table(client, [src_table1, src_table2], dst_table, delete_source=False)
        # show_table_data(client, dst_table)

        print("=" * 50, "Record & REPEATED Schema", "=" * 50)
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
        try:
            if CLEANING:
                # Cleaning
                print("=" * 50, "Cleaning ...", "=" * 50)
                client.delete_dataset(my_dataset.dataset_id, delete_contents=True, not_found_ok=True)
                print("Dataset {} deleted!".format(my_dataset.dataset_id))
                bucket = storage_client.get_bucket(bucket_name)
                bucket.delete(storage_client)
                print("Bucket {} deleted!".format(bucket.name))
        except Exception as error:
            logging.error("Error when cleaning resources.")
            raise error


def create_table(client, table_name, schema, project=None, dataset=None, partitioning_type=None, partitioned_field=None, clustering_fields=None):
    """
    Create Table with Schema
    :param client: BQ Client
    :param table_name: Table name
    :param schema: Table Schema
    :param project: default to client.project
    :param dataset: default to client.dataset
    :param partitioning_type: either : `time` or `range` partitioned
    :param partitioned_field: field name use for partitionning
    :param clustering_fields: fields to use for clustering
    :return: created table
    """
    partitioning_types = {
        "time" : TimePartitioning(type_= TimePartitioningType.HOUR, field=partitioned_field, require_partition_filter=True),
        "range" : RangePartitioning(range_= PartitionRange(start=0, end=100, interval=10), field=partitioned_field)
    }
    try:
        if project is None:
            project = client.project
        if dataset is None:
            dataset = client.dataset
        logging.info("Project: {}\tDataset: {}\tTable: {}\t\tPartitioning Type:{}".format(project, dataset.dataset_id, table_name, partitioning_type))
        table_id = "{}.{}.{}".format(project, dataset.dataset_id, table_name)
        table = bigquery.Table(table_id, schema=schema)
        if partitioning_type is not None:
            partitioning_type = partitioning_type.lower()
            if partitioning_type == "time":
                logging.info("Table Partitioning: {}".format(partitioning_type))
                schema.append(bigquery.SchemaField("ZONE_PARTITIONTIME","TIMESTAMP"))
                table.schema = schema
                table.time_partitioning = partitioning_types.get(partitioning_type)
            elif partitioning_type == "range":
                table.range_partitioning = partitioning_types.get(partitioning_type)
        if clustering_fields is not None:
            table.clustering_fields = clustering_fields
        client.create_table(table, exists_ok=True)
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


def insert_rows_from_object(client, dataset, table_name, table_rows, table_schema, table_rows_ids=None, create_table_bool=True, max_retries=3):
    """
    Insert rows to a table from a sequence
    :param client: BQ Client
    :param dataset: BQ Dataset
    :param table_name: The table string name
    :param table_rows: The sequence data
    :param table_schema: The table schema
    :param table_rows_ids: The table rows ids, default ot : range(len(table_row))
    :param create_table_bool: Boolean, default to True
    :param max_retries: Max retries, default to 3
    :return: The updated Table
    """
    try:
        print("Insert {} rows to Table: {}.{}".format(len(table_rows), dataset.dataset_id, table_name))
        full_table_id = "{}.{}.{}".format(client.project, dataset.dataset_id, table_name)
        logging.info("Full_Table_ID :: {}".format(full_table_id))
        table = create_table(client, table_name, table_schema,dataset=dataset) if create_table_bool else client.get_table(full_table_id)
        table_rows_ids = range(len(table_rows)) if table_rows_ids is None else table_rows_ids
        num_rows = 0
        retries = 0
        while num_rows == 0 and retries < max_retries:
            errors = client.insert_rows(table, table_rows, row_ids=table_rows_ids)
            print("Try to insert rows.\tErrors : {}".format(errors))
            table = client.get_table(table)
            num_rows = table.num_rows
            retries += 1
            time.sleep(3)
        return table
    except GoogleAPICallError as api_error:
            logging.error("Error occurred when calling BQ API:\nErrorCode: {}\tErrorMessage: {}\t".format(api_error.code,api_error.message))
    except Exception as error:
        logging.error("Can not insert rows in table.")
        logging.error(error)
        raise


def insert_rows_from_object_partition_cluster_aware(client, job_location, job_id, dataset, table_name, table_rows:tuple, table_schema, partitioning_type="time", clustering_fields=None, create_table_bool=True):
    """
    Insert rows to a table from a sequence considering table partitioning
    :param job_id:
    :param job_location:
    :param client: BQ Client
    :param dataset: BQ Dataset
    :param table_name: The table string name
    :param table_rows: The sequence data
    :param table_schema: The table schema
    :param table_rows_ids: The table rows ids, default ot : range(len(table_row))
    :param create_table_bool: Boolean, default to True
    :param max_retries: Max retries, default to 3
    :return: The updated Table
    """

    def update_row(rows):
        extended = []
        for row in rows:
            tuple_to_list = list(row)
            current_datetime_hour_granularity = datetime.utcnow().replace(minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")
            tuple_to_list.insert(0, current_datetime_hour_granularity)
            tuple_to_list.insert(len(tuple_to_list), current_datetime_hour_granularity)
            row = tuple(tuple_to_list)
            extended.append(row)
        extended=str(tuple(extended))
        return extended [1:-1]

    '''
    def fields_params(schema,rows):

        for field in schema:
            for row in rows:
                utcnow = datetime.utcnow()
                partition_time = utcnow.replace(hour=0,minute=0,second=0,microsecond=0)
                print(f"partition_time == {partition_time}")
                query_param = [   bigquery.ScalarQueryParameter('_PARTITIONTIME', 'TIMESTAMP', utcnow),
                                  bigquery.ScalarQueryParameter('name', 'STRING', row["name"]),
                                  bigquery.ScalarQueryParameter('age', 'INTEGER', row["age"]),
                                  bigquery.ScalarQueryParameter('dob', 'DATE', row["dob"]),
                                  bigquery.ScalarQueryParameter('ZONE_PARTITIONTIME', 'TIMESTAMP', utcnow)
                                ]
    def field_values(rows):
        values = []
        print(f"type row : {type(rows[0])}")
        if isinstance(rows[0], dict):
            values = [tuple(str(v) for k,v in d.items()) for d in rows]
        else:
            values = rows
        print(f"values:\n{values}")
        return values
    '''

    def field_names(schema):
        names = [field.name for field in schema]
        extended = "_PARTITIONTIME, " + ", ".join(names)
        return extended
    try:
        full_table_id = "{}.{}.{}".format(client.project, dataset.dataset_id, table_name)
        print(f"INSERT_ROWS_FROM_OBJECT_PARTITION_CLUSTER_AWARE : \t FULL_TABLE_ID : {full_table_id} \n\t\t PartitioningType : {partitioning_type} \t ClusteringFields : {clustering_fields}")
        if create_table_bool:
            table = create_table(client, table_name, table_schema,dataset=dataset, partitioning_type=partitioning_type, clustering_fields=clustering_fields)
        else:
            table = client.get_table(full_table_id)
        # Partition Time Table
        insert_query_str = """
        INSERT INTO `{}` ({})
        VALUES {}
        ;
        """
        if table.time_partitioning is not None:
            print(f"Table . TimePartitioning : {table.time_partitioning}")
            insert_query_str = insert_query_str.format(full_table_id,field_names(table_schema), update_row(table_rows))
        else:
            insert_query_str = insert_query_str.format(full_table_id,", ".join([field.name for field in table_schema]),str(tuple(table_rows))[1:-1])

        results = query_job_partition_aware(client, job_location, job_id, insert_query_str, use_partition_time_filter=False, cluster_fields=clustering_fields)
        return results
    except GoogleAPICallError as api_error:
        logging.error("Error occurred when calling BQ API:\nErrorCode: {}\tErrorMessage: {}\t".format(api_error.code,api_error.message))
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
    dataset = client.get_dataset(dataset_id)
    dataset.description = description
    client.update_dataset(dataset, ['description'])
    print("\n - {} \t Description: {}".format(dataset.dataset_id, dataset.description))
    return dataset


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


def create_dataset(client, dataset_id, location, project=None, timeout=30):
    """
    Create a dataset in the client default project
    :param client: BQ Client
    :param dataset_id: Dataset ID str
    :param location: BQ Dataset location
    :param project: BQ Project default to client.project
    :param timeout: TIMEOUT
    :return: Datset ID
    """
    if project is None:
        project = client.project
    dataset_id = "{}.{}".format(project, dataset_id)
    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location
    try:
        # Send the dataset to the API for creation, with an explicit timeout.
        dataset = client.create_dataset(dataset, timeout=timeout, exists_ok=True)  # Make an API request.
        print("Created dataset {}.{}".format(project, dataset.dataset_id))
    except Exception as error:
        logging.error("Failed to create dataset {}".format(dataset_id))
        logging.error(error)
        raise
    return dataset


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


def query_job_sample(client, job_location, job_id, query_str, query_params=None, dest_table_id=None,query_priority="interactive", cluster_fields=None):
    """
    Create and execute a query job
    :param client: BQ Client
    :param job_location: Job location
    :param job_id: Job ID
    :param query_str: The Query string
    :param dest_table_id: Optional destination table that receive the query result
    :param query_params : Optional query parameters if query is parametrized
    :param query_priority: Interactive by default or Batch
    :return: result job rows iterator
    """
    priorities = {"interactive": bigquery.QueryPriority.INTERACTIVE, "batch": bigquery.QueryPriority.BATCH}
    job_priority = priorities.get(query_priority.lower())
    logging.info('\t Query Job : Mode : {} \t'.format(job_priority))
    logging.info("Location: {}\tJob ID:{}\tDestination Table:{}".format(job_location, job_id, dest_table_id))
    logging.info("\t/---------------Query------------------\\")
    logging.info(query_str)
    logging.info("\t\---------------Query------------------/")
    sample_job_conf = bigquery.QueryJobConfig(
        use_legacy_sql=False,
        labels={"name": "bq_example"},
        priority=job_priority,
        query_parameters=query_params,
        destination=dest_table_id,
        dry_run=False,
        clustering_fiels=cluster_fields
        )
    query_job: QueryJob = client.query(
        location=job_location,
        query=query_str,
        job_config=sample_job_conf,
        job_id=job_id
    )
    # logging.info(query_job.query_parameters)
    # sample_job_conf.dry_run = True
    # size = int(query_job.total_bytes_processed / (8 * 1024 ) )
    # print("This query will process {} KB.".format(size))
    try:
        results = query_job.result()  # Waits for job to complete.
        wait(lambda: results) is results
        return results
    except TimeoutExpired:
        logging.error("Timeout waiting expired !")
        raise


def query_job_partition_aware(client, job_location, job_id, query_str, partition_time_filter:PartitionTimeFilter=None, zone_partition_time_filter:PartitionTimeFilter=None, use_partition_time_filter=False, use_zone_partition_time_filter=False, query_params=[], dest_table_id=None, query_priority="interactive", cluster_fields=None):
    try:
        query_str_extended = query_str
        if use_partition_time_filter:
            (exact_time, lower_bound_utc_datetime, upper_bound_utc_datetime) = (partition_time_filter.exact_time, partition_time_filter.lower_bound_datetime, partition_time_filter.upper_bound_datetime)
            if exact_time:
                query_str_extended = query_str + "WHERE _PARTITIONTIME = CAST('{}' AS TIMESTAMP)".format(exact_time)
            elif lower_bound_utc_datetime and not upper_bound_utc_datetime:
                query_str_extended = query_str + "WHERE _PARTITIONTIME >= CAST('{}' AS TIMESTAMP)".format(lower_bound_utc_datetime)
            elif upper_bound_utc_datetime and not lower_bound_utc_datetime:
                query_str_extended = query_str + "WHERE _PARTITIONTIME <= CAST('{}' AS TIMESTAMP)".format(upper_bound_utc_datetime)
            elif lower_bound_utc_datetime and upper_bound_utc_datetime:
                query_str_extended = query_str + "WHERE _PARTITIONTIME BETWEEN CAST('{}' AS TIMESTAMP) AND CAST('{}' AS TIMESTAMP)".format(lower_bound_utc_datetime, upper_bound_utc_datetime)
            if use_zone_partition_time_filter:
                (exact_time, lower_bound_utc_datetime, upper_bound_utc_datetime) = (zone_partition_time_filter.exact_time, zone_partition_time_filter.lower_bound_datetime, zone_partition_time_filter.upper_bound_datetime)
                if exact_time:
                    query_str_extended += "\n\tAND TIMESTAMP_ADD(ZONE_PARTITIONTIME, INTERVAL 1 HOUR) = CAST('{}' AS TIMESTAMP)".format(exact_time)
                elif lower_bound_utc_datetime and not upper_bound_utc_datetime:
                    query_str_extended += "\n\tAND TIMESTAMP_ADD(ZONE_PARTITIONTIME, INTERVAL 1 HOUR) >= CAST('{}' AS TIMESTAMP)".format(
                        lower_bound_utc_datetime)
                elif upper_bound_utc_datetime and not lower_bound_utc_datetime:
                    query_str_extended += "\n\tAND TIMESTAMP_ADD(ZONE_PARTITIONTIME, INTERVAL 1 HOUR) <= CAST('{}' AS TIMESTAMP)".format(
                        upper_bound_utc_datetime)
                elif lower_bound_utc_datetime and upper_bound_utc_datetime:
                    query_str_extended += "\n\tAND TIMESTAMP_ADD(ZONE_PARTITIONTIME, INTERVAL 1 HOUR) BETWEEN CAST('{}' AS TIMESTAMP) AND CAST('{}' AS TIMESTAMP)".format(
                        lower_bound_utc_datetime, upper_bound_utc_datetime)

        results = query_job_sample(client,job_location,job_id,query_str_extended,query_params, cluster_fields=cluster_fields)
        return results
    except Exception as error:
        logging.error("Can not extend query job with time based filter partition")
        logging.error(error)
        raise


def utc_to_local(utc_datetime:datetime, tz_info=TIME_ZONE):
    if isinstance(utc_datetime, datetime):
        local_datetime = utc_datetime.astimezone(tz=timezone(tz_info))
        return local_datetime
    else:
        return utc_datetime

def print_rows(results):
    result_list = list(results)
    print("Rows Count : {}".format(len(result_list)))
    for row in result_list:
        print(row),


def show_table_data(client, table, max_result=10):
    # Print row data in tabular format.
    rows = client.list_rows(table, max_results=max_result)
    format_string = "{!s:<20} " * len(rows.schema)
    field_names = [field.name for field in rows.schema]
    print(format_string.format(*field_names))  # Prints column headers.
    for row in rows:
        print(format_string.format(*row))  # Prints row data.


if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
    run()