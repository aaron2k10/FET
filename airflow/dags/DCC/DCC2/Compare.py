import pandas as pd
import os
from .models import Result
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
diff_df = pd.DataFrame(differences)
output_excel_file = 'media\JobResults\differences.xlsx'
diff_df.to_excel(output_excel_file, index=False)

print(diff_df)
Result.objects.all().delete()
#Result.objects.bulk_create([
        #Result(Row=row['Row'], Column=row['Column'],OriginalValue=row['Original Value'],ComparedValue=row['Compared Value']) for _, row in df.iterrows()
    #])
print(f"Differences successfully written to {output_excel_file}")
