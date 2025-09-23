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

print("inputfiles")
my_files = Files(profile)
conn_string = "postgresql://postgres:admin@127.0.0.1:5432/mydb"
db = create_engine(conn_string)
conn = db.connect()
query = "SELECT * FROM output_analysis;"
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
    if file_type.lower() == 'input':
        input_file_path = os.path.join(input_dir, file_name)
        try:
            my_files.ds.download(file_name, input_file_path)
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

    # Fetch and write the content for copybook files
    if copybook.lower() != 'copybook not found':
        #copybook_full_name = f"{copybook_library}({copybook.split('.')[0]})"
        copybook_file_path = os.path.join(copybook_dir, copybook)
        try:
            my_files.ds.download(copybook,copybook_file_path)
            #content = copybook_content["response"]
#            if isinstance(copybook_content, dict):
#                copybook_content = copybook_content.get('content', '')
        except Exception as e:
            print(f"Error fetching content for copybook {copybook}: {e}")

print("Directory and file contents created successfuly.")
