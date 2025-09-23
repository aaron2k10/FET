from zowe.zos_files_for_zowe_sdk import Files, DatasetOption
import pandas as pd
import subprocess
import re
import json
from sqlalchemy import create_engine
from coboljsonifier.copybookextractor import CopybookExtractor
from coboljsonifier.parser import Parser
from coboljsonifier.config.parser_type_enum import ParseType
import os

def parse_copybook(copybook_content):
    fields = {}
    lines = copybook_content.split('\n')
    for line in lines:
        if 'PIC' in line:
            parts = line.split()
            field_name = parts[1]
            match = re.search(r'X\((\d+)\)', line)
            if match:
                field_length = int(match.group(1))
                fields[field_name] = ' ' * field_length
    return fields
def parse1_copybook(copybook_path):
    extractor = CopybookExtractor(copybook_path)
    return extractor.dict_book_structure

def write_file(file_path, outfile_path, copybook_structure):
    parser = Parser(copybook_structure, ParseType.FLAT_ASCII).build()
    records = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            parser.parse(line)
            records.append(parser.value)
    with open(outfile_path, 'w', encoding='utf-8') as output_file:
        for record in records:
            output_file.write(json.dumps(record) + '\n')
# Profile configuration
profile = {
    "host": "192.86.32.87",
    "port": "10443",
    "user": "cgdevds",
    "password": "CAPGEMSK"
}

# Initialize Zowe and Files object
zos_files = Files(profile)
vsam_dataset = 'CGDEVPB.DCC.VSAMREAD.KSDS'
copybook = 'AWS.M5.CARDDEMO.CPY(READACCT)'
file_path = "Pyfiles/READACCT.txt"
zos_files.ds.download(vsam_dataset, file_path)
zos_files.ds.download(copybook, 'Copybook/vsamcpybk.cpy')

command = [
    'python', 'src/mdu.py', 'parse', 'Copybook/vsamcpybk.cpy', 'sample-json/vsamcpybk.json'
]
result = subprocess.run(command, capture_output=True, text=True)

with open('sample-json/vsamcpybk.json') as f:
    data = json.load(f)


datalist = []
with open(file_path, "r") as file:
    for line in file:
        record = {}
        for i in data['transf']:
            offset = i['offset']
            bytes = i['bytes'] + i['offset']
            name = i['name']
            record[name] = line[offset:bytes].strip()
        datalist.append(record)

df = pd.DataFrame(datalist)
conn_string = "postgresql://postgres:admin@127.0.0.1:5432/postgres"
db = create_engine(conn_string)
with db.connect() as conn:
    df.to_sql('READACCT', con=conn, if_exists='replace', index=False)

# Download the vsamdataset from PostgreSQL into a txt file using the same copybook

with open(file_path, "w") as file:
    for index, row in df.iterrows():
        line = ""
        for field in data['transf']:
            field_name = field['name']
            field_length = field['bytes']
            value = str(row[field_name]).ljust(field_length)
            line += value
        file.write(line + "\n")
zos_files.ds.download(copybook, f'Copybook/{copybook}.txt')
copybook_structure = parse1_copybook(f'Copybook/{copybook}.txt')
write_file(file_path, file_path, copybook_structure)
print(f"Data has been successfully written to {file_path}.")