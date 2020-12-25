import uuid
from datetime import date, time, datetime
from google.cloud import bigquery
from google.cloud.bigquery import table

def run():
    # Construct a BigQuery client object.
    client = bigquery.Client() #return QueryJob

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
    print("\tType: {}\n\tState: {}\n\tCreated: {}"
          .format(job.job_type, job.state, job.created)
          )

if __name__ == "__main__":
    run()    