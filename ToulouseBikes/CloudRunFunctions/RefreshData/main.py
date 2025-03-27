import functions_framework
from google.cloud import bigquery
import os

@functions_framework.http  
def live_data_clean(request):
    client = bigquery.Client()

    try:
        query1_path = os.path.join(os.getcwd(), "query_del_data.txt")
        query2_path = os.path.join(os.getcwd(), "query_merge_data.txt")

        if not os.path.exists(query1_path) or not os.path.exists(query2_path):
            return {"error": "Query files not found"}, 500

        with open(query1_path, "r") as file:
            query1 = file.read()

        with open(query2_path, "r") as file:
            query2 = file.read()

        job_config = bigquery.QueryJobConfig(priority=bigquery.QueryPriority.INTERACTIVE)

        query_job = client.query(query1, job_config=job_config)
        query_job.result() 
        print("Rows deleted successfully.")

        query_job = client.query(query2, job_config=job_config)
        query_job.result()  
        print("Table merged successfully.")

        return {"message": "Queries executed successfully"}, 200

    except Exception as e:
        print(f"Error: {str(e)}")
        return {"error": str(e)}, 500
