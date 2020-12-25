import uuid
import logging
from datetime import date, time, datetime
from google.cloud import bigquery
from google.cloud.bigquery import table

def run():
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
        LIMIT 10"""
    sample_job_conf=bigquery.QueryJobConfig(
        use_legacy_sql = False,
        labels = {"name":"bq_example"},
        time_partionning = table.TimePartitioning(),
        clustering = None
    )
    exec_date = datetime.utcnow()
    exec_date_fmt = "%Y_%m_%dT%H-%M-%S"
    exec_date_str = exec_date.strftime(exec_date_fmt)
    sample_job_id = f"big_query_sample_{exec_date_str}_{uuid.uuid4()}"
    job_location = "US"
    query_job = client.query(
        location=job_location,
        query=query_str,
        job_config=sample_job_conf,
        job_id=sample_job_id
    )
    results = query_job.result()  # Waits for job to complete.
    for row in results:
        print(row)
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
        dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
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

    print("="*30,"Deleting dataset ","="*30)
    #Deleting dataset
    client.delete_dataset(my_dataset.dataset_id, delete_contents=True, not_found_ok=True)
    print("Dataset {} deleted!".format(my_dataset.dataset_id))

if __name__ == "__main__":
    run()    