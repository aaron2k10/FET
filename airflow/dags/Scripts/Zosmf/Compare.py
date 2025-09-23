import pandas as pd
import time
from zowe.zos_files_for_zowe_sdk import Files, DatasetOption
from coboljsonifier.copybookextractor import CopybookExtractor
from coboljsonifier.parser import Parser
from coboljsonifier.config.parser_type_enum import ParseType
import re
import json,subprocess,os
import logging
import csv
import requests
from requests.auth import HTTPBasicAuth
import urllib3

# Disable SSL warnings (only for testing)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def compare_datalists(list1, list2, output_csv):
    mismatches = []

    # Determine the minimum length to avoid index errors
    min_length = min(len(list1), len(list2))

    for i in range(min_length):
        record1 = list1[i]
        record2 = list2[i]
        for key in record1:
            val1 = record1[key]
            val2 = record2.get(key)
            if val1 != val2:
                mismatches.append({
                    'line_number': i + 1,
                    'key': key,
                    'file1_value': val1,
                    'file2_value': val2
                })

    # Check for extra lines in either list
    if len(list1) > min_length:
        for i in range(min_length, len(list1)):
            mismatches.append({
                'line_number': i + 1,
                'key': 'N/A',
                'file1_value': list1[i],
                'file2_value': 'Missing'
            })
    elif len(list2) > min_length:
        for i in range(min_length, len(list2)):
            mismatches.append({
                'line_number': i + 1,
                'key': 'N/A',
                'file1_value': 'Missing',
                'file2_value': list2[i]
            })

    # Write mismatches to CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['line_number', 'key', 'file1_value', 'file2_value']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for mismatch in mismatches:
            writer.writerow(mismatch)

def compare_files(file1_path, file2_path, output_csv):
    # Read lines from both files
    with open(file1_path, 'r') as f1, open(file2_path, 'r') as f2:
        lines1 = f1.readlines()
        lines2 = f2.readlines()

    # Determine the maximum number of lines to compare
    max_lines = max(len(lines1), len(lines2))

    # List to store mismatched lines
    mismatches = []

    # Compare lines
    for i in range(max_lines):
        line1 = lines1[i].strip() if i < len(lines1) else ''
        line2 = lines2[i].strip() if i < len(lines2) else ''
        if line1 != line2:
            mismatches.append({
                'Line Number': i + 1,
                'File 1': line1,
                'File 2': line2
            })

    # Write mismatches to CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Line Number', 'File 1', 'File 2']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for mismatch in mismatches:
            writer.writerow(mismatch)


# Example usage
# command = [
#     'python', 'C:/Users/KALHARIS/PycharmProjects/DCC FET/myproject/src/mdu.py', 'parse', 'copybook.txt', 'copybook.json'
# ]
# result1 = subprocess.run(command, capture_output=True, text=True)
with open('copybook.json') as f:
    data = json.load(f)
def datalist(file):
    listdata=[]
    with open(f'{file}', "rb") as file:
        for line in file:
            record = {}
            for i in data['transf']:
                offset = i['offset']
                bytes = i['bytes'] + i['offset']
                name = i['name']
                record[name] = line[offset:bytes].strip()
            listdata.append(record)
        return listdata
file1_datalist=datalist('file1.txt')
file2_datalist=datalist('file2.txt')
mismatches = compare_datalists(file1_datalist, file2_datalist,'comp_mismatches.csv')




# Download the vsamdataset from PostgreSQL into a txt file using the same copybook

compare_files('file1.txt', 'file2.txt', 'line_mismatches.csv')
