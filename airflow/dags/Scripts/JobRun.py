import os
import time,json
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from zowe.zos_jobs_for_zowe_sdk import Jobs
from zowe.zos_files_for_zowe_sdk import Files
from coboljsonifier.copybookextractor import CopybookExtractor
from coboljsonifier.parser import Parser
from coboljsonifier.config.parser_type_enum import ParseType

def find_sysout_dd_names(jclfile):
    sysout_dd_names = []
    for line in jclfile.splitlines():
        if '//SYSOUT' in line:
            parts = line.split(',')
            for part in parts:
                if 'DSN=' in part:
                    sysout_dd_names.append(part.split('=')[1].strip())
    return sysout_dd_names

def parse_copybook(copybook_path):
    extractor = CopybookExtractor(copybook_path)
    return extractor.dict_book_structure

def write_file(file_path,outfile_path,copybook_structure):
    parser = Parser(copybook_structure, ParseType.FLAT_ASCII).build()
    records = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            parser.parse(line)
            records.append(parser.value)
    with open(outfile_path, 'w', encoding='utf-8') as output_file:
        for record in records:
            output_file.write(json.dumps(record) + '\n')

def submit_and_monitor_jobs(df, my_jobs, files,conn):
    job_results = []
    print(f"Jobs : {df}")
    job = my_jobs.submit_from_mainframe(df['Jobs'])
    job_name = job["jobname"]
    job_id = job["jobid"]
    print(f"Job {job_name} ID {job_id} submitted")
    job_results.append({"JOB_NAME": job_name, "JOB_ID": job_id, "RETURN_CODE": None})
    boolJobNotDone = True
    while boolJobNotDone:
        status = my_jobs.get_job_status(job_name, job_id)
        job_status = status["status"]
        if job_status != "OUTPUT":
            print(f"Job {job_name} Status: {job_status}")
            time.sleep(5)
        else:
            boolJobNotDone = False
            job_retcode = status["retcode"]
            if job_retcode == "CC 0000":
                jclfile = files.ds.get_content(df['Jobs'])
                query = "SELECT * FROM output_analysis1;"
                jbdtl = pd.read_sql(query, con=conn)
                print(jbdtl)
                for index, jb in jbdtl.iterrows():
                    if(jb['Job_Name']==df['Jobs'] and jb['File_Type']=='OUTPUT'):
                        files.ds.download(jb['File_Name'], f"/home/arunkua/airflow/dags/Scripts/MNFfiles/MNF.{jb['File_Name']}.txt")
                        files.ds.download(jb['Copybook_Name'], f"/home/arunkua/airflow/dags/Scripts/Copybook/{jb['Copybook_Name']}.txt")
                        copybook_structure = parse_copybook(f"/home/arunkua/airflow/dags/Scripts/Copybook/{jb['Copybook_Name']}.txt")
                        write_file(f"/home/arunkua/airflow/dags/Scripts/MNFfiles/MNF.{jb['File_Name']}.txt", f"/home/arunkua/airflow/dags/Scripts/MNFfiles/MNF.{jb['File_Name']}.txt", copybook_structure)
            print(f"Job {job_name} ID {job_id} ended with {job_retcode}")
            job_results[-1]["RETURN_CODE"] = job_retcode
    df = pd.DataFrame(job_results)
    df.to_sql('jobrunstatus', con=conn, if_exists='replace', index=False)

def main(df):
    profile = {
        "host": "192.86.32.87",
        "port": "10443",
        "user": "cgdevsk",
        "password": "cgdevsk"
    }

    my_jobs = Jobs(profile)
    files = Files(profile)
    conn_string = "sqlite:///DCCFET.db"
    db = create_engine(conn_string)
    conn = db.connect()


    submit_and_monitor_jobs(df, my_jobs, files,conn)
    conn.close()


    print("All jobs processed and results saved.")

if __name__ == "__main__":
    main()
"""
import os
import time,json
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from zowe.zos_jobs_for_zowe_sdk import Jobs
from zowe.zos_files_for_zowe_sdk import Files
from coboljsonifier.copybookextractor import CopybookExtractor
from coboljsonifier.parser import Parser
from coboljsonifier.config.parser_type_enum import ParseType

def find_sysout_dd_names(jclfile):
    sysout_dd_names = []
    for line in jclfile.splitlines():
        if '//SYSOUT' in line:
            parts = line.split(',')
            for part in parts:
                if 'DSN=' in part:
                    sysout_dd_names.append(part.split('=')[1].strip())
    return sysout_dd_names

def parse_copybook(copybook_path):
    extractor = CopybookExtractor(copybook_path)
    return extractor.dict_book_structure

def write_file(file_path,outfile_path,copybook_structure):
    parser = Parser(copybook_structure, ParseType.FLAT_ASCII).build()
    records = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            parser.parse(line)
            records.append(parser.value)
    with open(outfile_path, 'w', encoding='utf-8') as output_file:
        for record in records:
            output_file.write(json.dumps(record) + '\n')

def submit_and_monitor_jobs(df, my_jobs, files,conn):
    job_results = []
    for index, row in df.iterrows():
        job = my_jobs.submit_from_mainframe(row['Jobs'])
        job_name = job["jobname"]
        job_id = job["jobid"]
        copybook = row['Copybook']
        print(f"Job {job_name} ID {job_id} submitted")
        job_results.append({"JOB_NAME": job_name, "JOB_ID": job_id, "RETURN_CODE": None})

        boolJobNotDone = True
        while boolJobNotDone:
            status = my_jobs.get_job_status(job_name, job_id)
            job_status = status["status"]
            if job_status != "OUTPUT":
                print(f"Job {job_name} Status: {job_status}")
                time.sleep(5)
            else:
                boolJobNotDone = False
                job_retcode = status["retcode"]
                if job_retcode == "CC 0000":
                    jclfile = files.ds.get_content(row['Jobs'])
                    outputfiles = find_sysout_dd_names(jclfile)
                    if row['Jobs'] == 'CGDEVPB.SMF1.JCL(READACCT)':
                        files.ds.download('CGDEVPB.DCC.VSAMREAD.KSDS', '/home/kalharis/airflow/dags/Scripts/MNFfiles/MNF.READACCT.txt')
                        files.ds.download(copybook, f'/home/kalharis/airflow/dags/Scripts/Copybook/{copybook}.txt')
                        copybook_structure = parse_copybook(f'/home/kalharis/airflow/dags/Scripts/Copybook/{copybook}.txt')
                        write_file('/home/kalharis/airflow/dags/Scripts/MNFfiles/MNF.READACCT.txt', '/home/kalharis/airflow/dags/Scripts/MNFfiles/MNF.READACCT.txt', copybook_structure)
                    for outfile in outputfiles:
                        if row['Jobs'] == 'CGDEVPB.SMF1.JCL(PROCACCT)':
                            files.ds.download(outfile, '/home/kalharis/airflow/dags/Scripts/MNFfiles/MNF.PROCACCT.txt')
                            files.ds.download(copybook, f'/home/kalharis/airflow/dags/Scripts/Copybook/{copybook}.txt')
                            copybook_structure = parse_copybook(f'/home/kalharis/airflow/dags/Scripts/Copybook/{copybook}.txt')
                            write_file('/home/kalharis/airflow/dags/Scripts/MNFfiles/MNF.PROCACCT.txt', '/home/kalharis/airflow/dags/Scripts/MNFfiles/MNF.PROCACCT.txt', copybook_structure)
                        elif row['Jobs'] == 'CGDEVPB.SMF1.JCL(VBREAD)':
                            files.ds.download(outfile, '/home/kalharis/airflow/dags/Scripts/MNFfiles/MNF.CGDEVPB.DCC.VBNEW3.OUT.txt')
                            files.ds.download(copybook, f'/home/kalharis/airflow/dags/Scripts/Copybook/{copybook}.txt')
                            copybook_structure = parse_copybook(f'/home/kalharis/airflow/dags/Scripts/Copybook/{copybook}.txt')
                            write_file('/home/kalharis/airflow/dags/Scripts/MNFfiles/MNF.CGDEVPB.DCC.VBNEW3.OUT.txt', '/home/kalharis/airflow/dags/Scripts/MNFfiles/MNF.CGDEVPB.DCC.VBNEW3.OUT.txt',copybook_structure)
                print(f"Job {job_name} ID {job_id} ended with {job_retcode}")
                job_results[-1]["RETURN_CODE"] = job_retcode
    df = pd.DataFrame(job_results)
    df.to_sql('jobrunstatus', con=conn, if_exists='replace', index=False)

def main():
    profile = {
        "host": "192.86.32.87",
        "port": "10443",
        "user": "cgdevds",
        "password": "capgem20"
    }

    my_jobs = Jobs(profile)
    files = Files(profile)
    conn_string = "postgresql://postgres:admin@localhost:5432/mydb"
    db = create_engine(conn_string)
    conn = db.connect()
    file_path = "/home/kalharis/airflow/dags/Scripts/media/"
    def fetch_first_excel_filename(folder_path):
        for filename in os.listdir(folder_path):
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                return filename
        return None
    joballocation = fetch_first_excel_filename(file_path)
    if joballocation!=None:
        file_path+=joballocation
    print(file_path)
    df = pd.read_excel(file_path)
    print("Submitting jobs...")

    submit_and_monitor_jobs(df, my_jobs, files,conn)
    conn.close()


    print("All jobs processed and results saved.")

if __name__ == "__main__":
    main()
"""
