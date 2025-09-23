from zowe.zos_files_for_zowe_sdk import Files,DatasetOption
import re

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
zos_files = Files(profile)

# Define the source and target datasets
source_dataset = 'CGDEVPB.DCC.VBSAMPLE1eded'
target_dataset = 'CGDEVPB.DCC.VBCCOPY'
available=zos_files.ds.list(target_dataset)
if available.items:
    zos_files.ds.delete(target_dataset)
dataset_attributes =DatasetOption(
    like=source_dataset # Block size
)
# Create the new dataset
zos_files.ds.create(target_dataset,dataset_attributes)
# Copy the dataset
zos_files.ds.copy_data_set_or_member(source_dataset, target_dataset)

# Retrieve and parse the copybook
copybook_member = "AWS.M5.CARDDEMO.CPY(VBSAMPLE)"
copybook_content = zos_files.ds.get_content(copybook_member)
new_record = parse_copybook(copybook_content)

# Populate the new record fields
field_values = {
    'NAME': 'Sahana',
    'EMPLOYEE_NO': '000010',
    'LOCATION': 'Japan',
    'SEX': 'M',
    'DEGREE': 'B.E',
    'DESIGNATION': 'ASSOCIATE',
    'SALARY': '30000',
    'YEARS_OF_EXP': '4',
    'FILLER': 'SKD'
}

for field, value in field_values.items():
    if field in new_record:
        new_record[field] = value.ljust(len(new_record[field]))

# Convert the new record to string
record_data = ''.join(new_record.values())

# Read existing records from the target dataset
existing_records = zos_files.ds.get_content(target_dataset)

# Append the new record
# For VB datasets, ensure each record is properly formatted
updated_records = existing_records + record_data

# Write back all records to the target dataset
zos_files.ds.write(target_dataset, updated_records, encoding='utf-8')

# Verify the insertion
records = zos_files.ds.get_content(target_dataset)
