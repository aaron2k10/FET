
from zowe.zos_files_for_zowe_sdk import Files, DatasetOption
from coboljsonifier.copybookextractor import CopybookExtractor
from coboljsonifier.parser import Parser
from coboljsonifier.config.parser_type_enum import ParseType
import json,os
import logging
from dotenv import load_dotenv
load_dotenv()
# Configure logging
logging.basicConfig(level=logging.INFO)

# Profile configuration
profile = {
        "host": "192.86.32.87",
        "port": "10443",
        "user": os.getenv("USERNAME"),
        "password": os.getenv("PASSWORD")
    }

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

# Start time

try:
    # Initialize Zowe and Files object
    zos_files = Files(profile)

    # Define the source and target datasets
    source_dataset = 'CGDEVPB.DCC.NEW1'
    target_dataset = 'CGDEVPB.DCC.NEW3'
    copybook = 'DCC.FET.CPY(EMPCPY)'

    try:
        # Check if the target dataset exists and delete if it does
        available = zos_files.ds.list(target_dataset)
        if available.items:
            if available.items[0].dsname==target_dataset:
                zos_files.ds.delete(target_dataset)
    except Exception as e:
        logging.error(f"Error checking or deleting target dataset: {e}")
        raise

    try:
        # Create the new dataset
        dataset_attributes = DatasetOption(like=source_dataset)
        zos_files.ds.create(target_dataset, dataset_attributes)
    except Exception as e:
        logging.error(f"Error creating target dataset: {e}")
        raise

    try:
        # Copy the dataset
        zos_files.ds.copy_data_set_or_member(source_dataset, target_dataset)
        zos_files.ds.download(target_dataset, f'../Scripts/Pyfiles/CGDEVPB.DCC.NEW2.txt')
        zos_files.ds.download(copybook, f'../Scripts/Copybook/{copybook}.txt')
        copybook_structure = parse_copybook(f'../Scripts/Copybook/{copybook}.txt')
        write_file('../Scripts/Pyfiles/CGDEVPB.DCC.NEW2.txt', '../Scripts/Pyfiles/CGDEVPB.DCC.NEW2.txt', copybook_structure)
    except Exception as e:
        logging.error(f"Error copying dataset: {e}")
        raise
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")

