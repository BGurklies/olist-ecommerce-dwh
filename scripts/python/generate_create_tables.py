import pandas as pd
import os

csv_folder = r'D:\Code\Datasets\brazilian_ecommerce_dataset_olist'
output_file = r'sql\02_create_raw_tables.sql'

sql = "USE OlistDWH;\nGO\n\n"

for file in sorted(os.listdir(csv_folder)):
    if file.endswith('.csv'):
        table_name = (file
            .replace('olist_', '')
            .replace('_dataset', '')
            .replace('.csv', ''))

        df = pd.read_csv(
            os.path.join(csv_folder, file),
            nrows=0, 
            encoding='utf-8'
        )

        columns = ',\n    '.join([f"[{col}] NVARCHAR(255)" for col in df.columns])

        sql += f"-- {file}\n"
        sql += f"CREATE TABLE raw.[{table_name}] (\n    {columns}\n);\nGO\n\n"

os.makedirs('sql', exist_ok=True)

with open(output_file, 'w', encoding='utf-8') as f:
    f.write(sql)
