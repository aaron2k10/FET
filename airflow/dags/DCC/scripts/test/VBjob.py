from zowe.zos_files_for_zowe_sdk import Files, DatasetOption
from coboljsonifier.copybookextractor import CopybookExtractor
from coboljsonifier.parser import Parser
from coboljsonifier.config.parser_type_enum import ParseType
import re,json
# Profile configuration
profile = {
    "host": "192.86.32.87",
    "port": "10443",
    "user": "cgdevds",
    "password": "CAPGEMSK"
}

# Initialize Zowe and Files object
zos_files = Files(profile)
def parse_copybook(copybook_path):
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
try:
    # Define the source and target datasets
    source_dataset = 'CGDEVPB.DCC.VBNEW1.OUT'
    target_dataset = 'CGDEVPB.DCC.VBNEW3.OUT'
    copybook = 'AWS.M5.CARDDEMO.CPY(VBCPY)'
    available = zos_files.ds.list(target_dataset)
    if available.items:
        zos_files.ds.delete(target_dataset)

    dataset_attributes = DatasetOption(like=source_dataset)  # Block size
    # Create the new dataset
    zos_files.ds.create(target_dataset, dataset_attributes)
    # Copy the dataset
    zos_files.ds.copy_data_set_or_member(source_dataset, target_dataset)
    zos_files.ds.download(target_dataset, f'Pyfiles/{target_dataset}.txt')
    zos_files.ds.download(copybook, f'Copybook/{copybook}.txt')
    copybook_structure = parse_copybook(f'Copybook/{copybook}.txt')
    write_file(f'Pyfiles/{target_dataset}.txt', f'Pyfiles/{target_dataset}.txt', copybook_structure)
except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Ensure connections are closed if necessary
    pass