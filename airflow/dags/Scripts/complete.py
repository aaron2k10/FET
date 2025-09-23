import os
import re
import pandas as pd
from zowe.zos_files_for_zowe_sdk import Files

# Define directories and profile
jcl_directory = "DCC.FET.JCL"
cbl_directory = "DCC.FET.COBOL"
proc_directory = r"C:\Users\KALHARIS\PycharmProjects\DCC FET\DCCLAT\DCCLAT\DCC\aws-mainframe-modernization-carddemo\aws-mainframe-modernization-carddemo\app\proc"
copybook_directory = "DCC.FET.CPY"
profile = {
    "host": "192.86.32.87",
    "port": "10443",
    "user": "cgdevds",
    "password": "capgem20"
}
zos_files = Files(profile)


def extract_pgm_name(jcl_content):
    match = re.search(r'EXEC PGM=([A-Z0-9]+)', jcl_content)
    return match.group(1) if match else None


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
                input_filenames.append((jcl_path, pgm_name, input_file_name, copybook_name, "INPUT"))
                copybook_mapping[jcl_path] = copybook_name
                print(
                    f'JCL: {jcl_path}, Program Name: {pgm_name}, File Name: {file_name}, File Structure: {file_structure}, Input File Name: {input_file_name}, Copybook: {copybook_name}')
            else:
                input_filenames.append((jcl_path, pgm_name, input_file_name, None, "INPUT"))
                print(
                    f'JCL: {jcl_path}, Program Name: {pgm_name}, File Name: {file_name}, File Structure: {file_structure}, Input File Name: {input_file_name}, Copybook: Not Found')

    sysout_dsns = extract_sysout_dsn(jcl_content)
    for sysout_dsn in sysout_dsns:
        copybook_name = copybook_mapping.get(jcl_path, None)
        input_filenames.append((jcl_path, pgm_name, sysout_dsn, copybook_name, "OUTPUT"))
        print(f'JCL: {jcl_path}, Program Name: {pgm_name}, SYSOUT DSN: {sysout_dsn}, Copybook: {copybook_name}')

    return input_filenames


def main():
    # Read Excel sheet
    #excel_file_path = r"C:/Users/KALHARIS/PycharmProjects/DCC FET/DCCLAT/DCCLAT/DCC/media/Job Allocation1.xlsx"
    file_path = r"../Scripts/media/"
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
    job_list = df['Job'].tolist()
    all_input_filenames = []
    for job in job_list:
        jcl_path = f"{jcl_directory}({job})"
        input_filenames = process_jcl_and_cobol(jcl_path)
        all_input_filenames.extend(input_filenames)

    print(f'All Input and Output File Names and Copybooks: {all_input_filenames}')


if __name__ == '__main__':
    main()