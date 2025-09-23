import os
import json
import pandas as pd
from sqlalchemy import create_engine
import sqlite3
def compare():
    mnf_file_path = "/home/arunkua/home/arunkua/dags/dags/Scripts/MNFfiles"
    py_file_path = "/home/arunkua/home/arunkua/dags/dags/Scripts/Pyfiles"
    filelist = []
    conn_string = "sqlite:///DCCFET.db"
    db = create_engine(conn_string)
    conn = db.connect()
    for pyfilename in os.listdir(py_file_path):
        mnffilename = "MNF." + pyfilename
        if mnffilename in os.listdir(mnf_file_path):
            mnffile_path = os.path.join(mnf_file_path, mnffilename)
            pyfile_path = os.path.join(py_file_path, pyfilename)
            mismatches = []

            with open(mnffile_path, 'r') as f1, open(pyfile_path, 'r') as f2:
                file1_lines = f1.readlines()
                file2_lines = f2.readlines()

                max_lines = max(len(file1_lines), len(file2_lines))

                for line_num in range(max_lines):
                    record1 = json.loads(file1_lines[line_num]) if line_num < len(file1_lines) else None
                    record2 = json.loads(file2_lines[line_num]) if line_num < len(file2_lines) else None

                    if record1:
                        for key, value in record1.items():
                            if value is None:
                                record1[key] = ""

                    if record2:
                        for key, value in record2.items():
                            if value is None:
                                record2[key] = ""

                    if record1 is None:
                        mismatches.append({
                            "line_number": line_num + 1,
                            "field_name": "Extra line in Python",
                            "file1_value": "No corresponding line",
                            "file2_value": ' '.join(record2.values())
                        })
                    elif record2 is None:
                        mismatches.append({
                            "line_number": line_num + 1,
                            "field_name": "Extra line in Mainframe",
                            "file1_value": ' '.join(record1.values()),
                            "file2_value": "No corresponding line"
                        })
                    else:
                        for key in record1.keys():
                            if record1[key] != record2[key]:
                                mismatches.append({
                                    "line_number": line_num + 1,
                                    "field_name": key,
                                    "file1_value": record1[key],
                                    "file2_value": record2[key]
                                })
            diff_df = pd.DataFrame(mismatches)
            # diff_df.to_sql(pyfilename, con=db, if_exists='replace',index=False)
            if diff_df.empty:
            # Create an empty DataFrame with the desired columns
                empty_df = pd.DataFrame(columns=["line_number", "field_name", "file1_value", "file2_value"])
                empty_df.to_sql(pyfilename, con=db, if_exists='replace', index=False)
            else:
                diff_df.to_sql(pyfilename, con=db, if_exists='replace', index=False)

            if not mismatches:
                filelist.append({
                    "file1":mnffilename,
                    "file1length":len(file1_lines),
                    "file2":pyfilename,
                    "file2length":len(file2_lines),
                    "Output":"Matched"
                    #"results": output_data
                })
            else:
                filelist.append({
                    "file1":mnffilename,
                    "file1length":len(file1_lines),
                    "file2":pyfilename,
                    "file2length":len(file2_lines),
                    "Output":"Mismatched",
                    #"results": output_data
                })
    complist = pd.DataFrame(filelist)
    print(filelist)
    complist.to_sql('filelist', con=db, if_exists='replace',index=False)
    conn.close()