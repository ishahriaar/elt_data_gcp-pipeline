from google.cloud import bigquery

def load_file(resource, client):
    table_id = f"myeltlearn.mydata23.dummy_{resource}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, 
        autodetect=True,
        write_disposition="write_truncate"
    )

    with open(f"{resource}.json", "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

        job.result()  # Waits for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

client =  bigquery.Client()
load_file("carts", client)
# load_file("users", client)

query= """
SELECT 
  u.id AS user_id,
  CONCAT (u.firstName, ' ' ,u.lastName) AS user_name,
  SUM(total) AS total_spent
FROM `mydata23.dummy_users` u
LEFT JOIN `mydata23.dummy_carts` c
ON u.id = c.userId
GROUP BY user_id, user_name
"""

query_config= bigquery.QueryJobConfig(
    destination = "myeltlearn.mydata23.most_spending_users", 
    write_disposition= "write_truncate"
    )
client.query(query, job_config= query_config)