import os
import time
import pandas as pd
import psycopg2
from pandas.io.common import file_path_to_url
from sqlalchemy import create_engine
from zowe.zos_jobs_for_zowe_sdk import Jobs
from zowe.zos_files_for_zowe_sdk import Files


JLIB = ''  # Input variable for the JCL library
profile = {
    "host": "192.86.32.87",
    "port": "10443",
    "user": "cgdevds",
    "password": "capgem16"
}

my_jobs = Jobs(profile)
files= Files(profile)
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="postgres",
    user="postgres",
    password="admin"
)

# Cursor to execute queries
cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS jobrunstatus (
        JOB_NAME CHAR(10),
        JOB_ID CHAR(10),
        RETURN_CODE CHAR(10)
    )
""")
conn.commit()

# Close the connection
cur.close()
conn.close()

conn_string = "postgresql://postgres:admin@127.0.0.1:5432/postgres"
#conn_string = "postgresql://{0}:{1}@{2}:{3}/{4}".format(
#            user, password, host, port, database
#       )
db = create_engine(conn_string)
conn = db.connect()
#output_file = "C:\\Users\KALHARIS\PycharmProjects\DCC FET\DCCLAT\DCCLAT\DCC\media\JobResults.xlsx"
file_path="C:\\Users\KALHARIS\PycharmProjects\DCC FET\DCCLAT\DCCLAT\DCC\media\Job Allocation.xlsx"
job_results = []
df = pd.read_excel(file_path)
print("Submitting jobs...")
for index, row in df.iterrows():
    # Submit the job
    job = my_jobs.submit_from_mainframe(row['Jobs'])
    job_name = job["jobname"]
    job_id = job["jobid"]
    print(f"Job {job_name} ID {job_id} submitted")

    # Record the job name and job ID in the job results list
    job_results.append({"JOB_NAME": job_name, "JOB_ID": job_id, "RETURN_CODE": None})
    # Save the job name and ID
    df = pd.DataFrame(job_results)
    df.to_sql('jobrunstatus', con=conn, if_exists='replace',
              index=False)

    # Wait until the job completes and get the return code
    boolJobNotDone = True
    while boolJobNotDone:
        status = my_jobs.get_job_status(job_name, job_id)
        job_status = status["status"]
        if job_status != "OUTPUT":
            print(f"Job {job_name} Status: {job_status}")
            time.sleep(5)  # Adjust sleep time as necessary
        else:
            boolJobNotDone = False
            job_retcode = status["retcode"]
            print(f"Job {job_name} ID {job_id} ended with {job_retcode}")

            # Update the return code in the job results list
            job_results[-1]["RETURN_CODE"] = job_retcode

            # Save the updated return code to the Excel sheet

            df = pd.DataFrame(job_results)
            df.to_sql('jobrunstatus', con=conn, if_exists='replace',
                      index=False)
# conn.commit()
conn.close()

print("All jobs processed and results saved.")
