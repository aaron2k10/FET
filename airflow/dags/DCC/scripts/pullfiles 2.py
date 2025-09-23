import os
import pandas as pd
from zowe.zos_files_for_zowe_sdk import Files
from sqlalchemy import create_engine

profile = {
    "host": "192.86.32.87",
    "port": "10443",
    "user": "cgdevds",
    "password": "CAPGEMSK"
}


my_files = Files(profile)
conn_string = "postgresql://postgres:admin@127.0.0.1:5432/postgres"
db = create_engine(conn_string)
conn = db.connect()
query = "SELECT * FROM output_analysis1;"
jbdtl = pd.read_sql(query, con=conn)
conn.close()
copybook_library = 'AWS.M5.CARDDEMO.CPY'


# Iteration
for _, row in jbdtl.iterrows():
    job_name = row['Job_Name']
    file_type = row['File_Type']
    file_name = row['File_Name']
    copybook = row['Copybook_Name']

    # Create the directory
    job_dir = os.path.join(job_name)
    input_dir = os.path.join(job_dir, 'input')
    output_dir = os.path.join(job_dir, '../../samples/output')
    copybook_dir = os.path.join(job_dir, 'copybook')

    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(copybook_dir, exist_ok=True)

    # Fetch and write the content for I/P
    if file_type.lower() == 'output':
        output_file_path = os.path.join(output_dir, file_name)
        try:
            my_files.ds.download(file_name, output_file_path)
            # content = file_content["response"]
#            print(file_content)
            # Handle the case where the content is a dictionary
#            if isinstance(file_content, dict):
#                file_content = file_content.get('content', '')
#                print(file_content)
#             with open(input_file_path, 'w') as f:
#                 f.write(file_content)
        except Exception as e:
            print(f"Error fetching content for input file {file_name}: {e}")
print("Directory and file contents created successfuly.")