import os
import re
import pandas as pd
from zowe.zos_files_for_zowe_sdk import Files
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()
# Define directories and profile
jcl_directory = "DCC.FET.JCL"
cbl_directory = "DCC.FET.COBOL"
#proc_directory = r"C:\Users\KALHARIS\PycharmProjects\DCC FET\DCCLAT\DCCLAT\DCC\aws-mainframe-modernization-carddemo\aws-mainframe-modernization-carddemo\app\proc"
copybook_directory = "DCC.FET.CPY"
profile = {
    "host": "192.86.32.87",
    "port": "10443",
    "user": os.getenv("USERNAME"),
    "password": os.getenv("PASSWORD")
}
zos_files = Files(profile)

def extract_pgm_name(jcl_content):
    lines = jcl_content.split('\n')
    for line in lines:
        match = re.search(r'EXEC PGM=([A-Z0-9]+)', line)
        pgm_name = match.group(1) if match else None
        if pgm_name and pgm_name != 'IEFBR14':
            return pgm_name


def extract_select_statements(cobol_content):
    select_statements = re.findall(r'SELECT\s+(\w+)\s+ASSIGN\s+TO\s+(\w+)', cobol_content)
    return {file_name: file_structure for file_name, file_structure in select_statements}


def extract_input_file_name(jcl_content, file_structure):
    pattern = rf'{file_structure}\s+DD\s+DSN=([^,]+)'
    match = re.search(pattern, jcl_content)
    return match.group(1) if match else None


def check_fd_and_copybook(cobol_content, file_name):
    fd_pattern = rf'FD\s+{file_name}'
    copy_pattern = rf'COPY\s+(\w+)'
    print(copy_pattern)
    fd_match = re.search(fd_pattern, cobol_content)
    if fd_match:
        copy_match = re.search(copy_pattern, cobol_content)
        if copy_match:
            copybook_name = copy_match.group(1)
            copybook_path = f"{copybook_directory}({copybook_name})"
            try:
                copybook_content = zos_files.ds.get_content(copybook_path)
                return copybook_name
            except Exception as e:
                print(f'Copybook {copybook_name} not found: {e}')
                return None
    return None


def extract_sysout_dsn(jcl_content):
    pattern = r'SYSOUT\s+DD\s+DSN=([^,]+)'
    matches = re.findall(pattern, jcl_content)
    return matches


def process_jcl_and_cobol(jcl_path):
    try:
        jcl_content = zos_files.ds.get_content(jcl_path)
    except Exception as e:
        print(f'Error reading JCL {jcl_path}: {e}')
        return []

    pgm_name = extract_pgm_name(jcl_content)
    if not pgm_name:
        print(f'PGM name not found in {jcl_path}')
        return []

    cobol_path = f"{cbl_directory}({pgm_name})"
    try:
        cobol_content = zos_files.ds.get_content(cobol_path)
    except Exception as e:
        print(f'Error reading COBOL program {pgm_name}: {e}')
        return []

    select_statements = extract_select_statements(cobol_content)
    input_filenames = []
    copybook_mapping = {}
    for file_name, file_structure in select_statements.items():
        input_file_name = extract_input_file_name(jcl_content, file_structure)
        if input_file_name:
            copybook_name = check_fd_and_copybook(cobol_content, file_name)
            if copybook_name:
                input_filenames.append((jcl_path, cobol_path, input_file_name,f"{copybook_directory}({copybook_name})", "INPUT"))
                copybook_mapping[jcl_path] = copybook_name
                print(
                    f'JCL: {jcl_path}, Program Name: {pgm_name}, File Name: {file_name}, File Structure: {file_structure}, Input File Name: {input_file_name}, Copybook: {copybook_name}')
            else:
                input_filenames.append((jcl_path,cobol_path, input_file_name, None, "INPUT"))
                print(
                    f'JCL: {jcl_path}, Program Name: {pgm_name}, File Name: {file_name}, File Structure: {file_structure}, Input File Name: {input_file_name}, Copybook: Not Found')

    sysout_dsns = extract_sysout_dsn(jcl_content)
    for sysout_dsn in sysout_dsns:
        copybook_name = copybook_mapping.get(jcl_path, None)
        input_filenames.append((jcl_path,cobol_path, sysout_dsn,f"{copybook_directory}({copybook_name})", "OUTPUT"))
        print(f'JCL: {jcl_path}, Program Name: {pgm_name}, SYSOUT DSN: {sysout_dsn}, Copybook: {copybook_name}')

    return input_filenames


def save_to_excel(output_data):
    df = pd.DataFrame(output_data, columns=['Job_Name', 'Program_Name', 'File_Name', 'Copybook_Name', 'File_Type'])
    conn_string = os.getenv("SQLITE_DB")
    db = create_engine(conn_string)
    conn = db.connect()
    df.to_sql('output_analysis', con=conn, if_exists='replace', index=False)
    conn.close()


def main():
    import os
    import pandas as pd

    # Read Excel sheet
    excel_file_path = r"../Scripts/media/"

    def fetch_first_excel_filename(folder_path):
        for filename in os.listdir(folder_path):
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                return filename
        return None

    joballocation = fetch_first_excel_filename(excel_file_path)

    if joballocation is not None:
        file_path = os.path.join(excel_file_path, joballocation)
    else:
        print("No Excel file found.")
        return

    print(f"Reading Excel file from: {file_path}")
    df = pd.read_excel(file_path)
    job_list = df['Jobs'].tolist()

    all_input_filenames = []

    for job in job_list:
        input_filenames = process_jcl_and_cobol(job)
        all_input_filenames.extend(input_filenames)

    print(f'All Input and Output File Names and Copybooks: {all_input_filenames}')

    save_to_excel(all_input_filenames)


if __name__ == '__main__':
    main()
