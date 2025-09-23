from zowe.zos_files_for_zowe_sdk import Files, DatasetOption
from coboljsonifier.copybookextractor import CopybookExtractor
from coboljsonifier.parser import Parser
from coboljsonifier.config.parser_type_enum import ParseType
import json
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)

# Profile configuration
profile = {
    "host": "192.86.32.87",
    "port": "10443",
    "user": "cgdevds",
    "password": "CAPGEMSK"
}

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
    except Exception as e:
        logging.error(f"Error copying dataset: {e}")
        raise

    try:
        # Extract the copybook
        copybookdata=zos_files.ds.get_content(copybook)
        print(copybookdata)
        copybook_extractor = CopybookExtractor(copybookdata)
        copybook_structure = copybook_extractor.extract()
        # Parse the dataset
        parser = Parser(copybook_structure, ParseType.DATASET)
        parsed_data = parser.parse(target_dataset)

        # Update the EMP-ID field
        for record in parsed_data:
            record['EMP-ID'] = 'E000X'

        # Convert updated data to JSON
        updated_data_json = json.dumps(parsed_data)

        # Write back to the target dataset
        zos_files.ds.write(target_dataset, updated_data_json)
    except Exception as e:
        logging.error(f"Error updating dataset: {e}")
        raise
except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")