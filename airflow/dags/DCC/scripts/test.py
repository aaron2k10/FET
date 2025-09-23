
import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine

# File paths
folder_with_headers = 'C:\\Users\KALHARIS\Downloads\with-headers'
folder_without_headers = 'C:\\Users\KALHARIS\Downloads\without-headers'

file_pattern = f"{folder_with_headers}/*xlsx"
for file in os.listdir(folder_with_headers):
    if file.endswith('.xlsx'):
        file_with_headers=f"{folder_with_headers}/{file}"
        file_without_headers = f"{folder_without_headers}/WH.{file}"
    df1 = pd.read_excel(file_with_headers, header=0)
    df2 = pd.read_excel(file_without_headers, header=None)
    df2.columns = df1.columns
    differences = []

    # Iterations
    for index, row in df1.iterrows():
        for col in df1.columns:
            if index < len(df2) and col in df2.columns:
                if row[col] != df2.at[index, col]:
                    differences.append({
                        "Row": index + 1,  # +1 to match Excel's 1-based index
                        "Column": col,
                        "Compared Value": row[col],
                        "Original Value": df2.at[index, col]
                    })
            else:
                differences.append({
                    "Row": index + 1,
                    "Column": col,
                    "Compared Value": row[col],
                    "Original Value": None
                })
    print(differences)
    diff_df = pd.DataFrame(differences)
    conn_string = "postgresql://postgres:admin@127.0.0.1:5432/postgres"
    db = create_engine(conn_string)
    conn = db.connect()
    diff_df.to_sql(file, con=conn, if_exists='replace',
              index=False)
    conn.close()
