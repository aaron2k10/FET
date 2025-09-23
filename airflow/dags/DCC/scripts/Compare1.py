import os
import json
import logging
from coboljsonifier.copybookextractor import CopybookExtractor
from coboljsonifier.parser import Parser
from coboljsonifier.config.parser_type_enum import ParseType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_copybook(copybook_path):
    try:
        extractor = CopybookExtractor(copybook_path)
        return extractor.dict_book_structure
    except Exception as e:
        logging.error(f"Error parsing copybook: {e}")
        raise

def read_file(file_path, copybook_structure):
    try:
        parser = Parser(copybook_structure, ParseType.FLAT_ASCII).build()
        records = []
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                parser.parse(line)
                records.append(parser.value)
        return records
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        raise

def compare_files(file1, file2, copybook_structure):
    try:
        data1 = read_file(file1, copybook_structure)
        data2 = read_file(file2, copybook_structure)
        mismatches = []
        max_lines = max(len(data1), len(data2))

        for index in range(max_lines):
            record1 = data1[index] if index < len(data1) else {}
            record2 = data2[index] if index < len(data2) else {}

            for field_name in copybook_structure.keys():
                value1 = record1.get(field_name, "N/A")
                value2 = record2.get(field_name, "N/A")
                if value1 != value2:
                    mismatches.append({
                        "line": index + 1,
                        "field": field_name,
                        "file1_value": value1,
                        "file2_value": value2
                    })
        return mismatches
    except Exception as e:
        logging.error(f"Error comparing files: {e}")
        raise

if __name__ == "__main__":
    copybook_path = 'C:/Users/KALHARIS/PycharmProjects/DCC FET/DCCLAT/DCCLAT/DCC/Copybook/vsamcpybk.cpy'
    file1_path = 'C:/Users/KALHARIS/PycharmProjects/DCC FET/DCCLAT/DCCLAT/DCC/MNFfiles/MNF.VSAMDATASET.txt'
    file2_path = 'C:/Users/KALHARIS/PycharmProjects/DCC FET/DCCLAT/DCCLAT/DCC/Pyfiles/VSAMDATASET.txt'

    try:
        copybook_structure = parse_copybook(copybook_path)
        mismatches = compare_files(file1_path, file2_path, copybook_structure)

        for mismatch in mismatches:
            print(f"Line {mismatch['line']}: Field '{mismatch['field']}' mismatch")
            print(f"\tFile 1: {mismatch['file1_value']}")
            print(f"\tFile 2: {mismatch['file2_value']}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")