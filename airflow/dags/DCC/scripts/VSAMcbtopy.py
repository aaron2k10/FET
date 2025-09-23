from zowe.zos_files_for_zowe_sdk import Files, DatasetOption
from zowe.zos_jobs_for_zowe_sdk import Jobs
import re
import time

def parse_copybook(copybook_content):
    fields = {}
    lines = copybook_content.split('\n')
    for line in lines:
        if 'PIC' in line:
            parts = line.split()
            field_name = parts[1]
            # Extract the length using regex to handle different formats
            match = re.search(r'X\((\d+)\)', line)
            if match:
                field_length = int(match.group(1))
                fields[field_name] = ' ' * field_length
    return fields

# Profile configuration
profile = {
    "host": "192.86.32.87",
    "port": "10443",
    "user": "cgdevds",
    "password": "capgem16"
}

# Initialize Zowe and Files object
try:
    zos_files = Files(profile)
    zos_jobs = Jobs(profile)
except Exception as e:
    print(f"Error initializing Zowe objects: {e}")
    exit(1)

# Define the source and target datasets
source_dataset = 'CGDEVPB.DCC.FBSAMPL1'
copy_dataset = 'CGDEVPB.DCC.FBCOPY1'
vsam_dataset = 'AWS.DCC.VSAM.FILE3'
dataset_attributes = DatasetOption(
    like=source_dataset  # Block size
)

try:
    available = zos_files.ds.list(copy_dataset)
    if available.items:
        zos_files.ds.delete(copy_dataset)
    zos_files.ds.create(copy_dataset, dataset_attributes)
except Exception as e:
    print(f"Error handling datasets: {e}")
    exit(1)

copybook_member = "AWS.M5.CARDDEMO.CPY(FBSAMPLE)"
try:
    copybook_content = zos_files.ds.get_content(copybook_member)
except Exception as e:
    print(f"Error retrieving copybook content: {e}")
    exit(1)

field_values = {
    'NAME': 'Praveen',
    'EMPLOYEE_NO': '000464',
    'LOCATION': 'China',
    'SEX': 'M',
    'DEGREE': 'B.E',
    'DESIGNATION': 'Associate',
    'SALARY': '70000',
    'YEARS_OF_EXP': '4',
    'FILLER': 'UUU'
}

new_record = parse_copybook(copybook_content)
for field, value in field_values.items():
    if field in new_record:
        new_record[field] = value.ljust(len(new_record[field]))

# Convert the new record to string
record_data = ''.join(new_record.values())

try:
    existing_records = zos_files.ds.get_content(source_dataset)
except Exception as e:
    print(f"Error retrieving existing records: {e}")
    exit(1)

# Append the new record
# For VB datasets, ensure each record is properly formatted
updated_records = existing_records + record_data

try:
    # Write back all records to the target dataset
    zos_files.ds.write(copy_dataset, updated_records, encoding='utf-8')
except Exception as e:
    print(f"Error writing to target dataset: {e}")
    exit(1)

# Verify the insertion
try:
    records = zos_files.ds.get_content(copy_dataset)
except Exception as e:
    print(f"Error verifying insertion: {e}")
    exit(1)

jcl = f"""
//CGDEVPB1 JOB 'Delete define Account Data',CLASS=A,MSGCLASS=0,
// NOTIFY=&SYSUID                                              
//*                                                            
//STEP05 EXEC PGM=IDCAMS                                       
//SYSPRINT DD   SYSOUT=*                                       
//SYSIN    DD   *                                              
   DELETE {vsam_dataset} -                                     
          CLUSTER                                              
   IF MAXCC LE 08 THEN SET MAXCC = 0                           
/*                                                             
//STEP10 EXEC PGM=IDCAMS                                       
//SYSPRINT DD   SYSOUT=*                                       
//SYSIN    DD   *                                              
   DEFINE CLUSTER (NAME({vsam_dataset}) -                      
          TRACKS(10,10) -                                      
          VOLUMES(VPWRKH -                                     
          ) -                                                  
          KEYS(5 0) -                                          
          RECORDSIZE(80 80) -                                  
          SHAREOPTIONS(2 3) -                                  
          ERASE -                                              
          INDEXED -                                            
          ) -                                                  
          DATA (NAME({vsam_dataset}.DATA) -                    
          ) -                                                  
          INDEX (NAME({vsam_dataset}.INDEX) -                  
          )                                                    
//STEP15 EXEC PGM=IDCAMS
//SYSPRINT DD   SYSOUT=*
//ACCTDATA DD DISP=SHR,
//         DSN={copy_dataset}
//ACCTVSAM DD DISP=SHR,
//         DSN={vsam_dataset}
//SYSIN    DD   *
   REPRO INFILE(ACCTDATA) OUTFILE(ACCTVSAM) REPLACE
/*
"""

try:
    jclsubmit = zos_jobs.submit_plaintext(jcl)
    jclstatus = zos_jobs.get_job_status(jclsubmit.jobname, jclsubmit.jobid)
    while jclstatus.retcode is None:
        time.sleep(5)
        jclstatus = zos_jobs.get_job_status(jclsubmit.jobname, jclsubmit.jobid)
    print(f"JobID: {jclstatus.jobid} Return Code: {jclstatus.retcode}")
except Exception as e:
    print(f"Error submitting or monitoring JCL job: {e}")