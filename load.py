import mysql.connector as sc
import pandas as pd
import os
import shutil
import glob

# ------------------------------------
# FOLDERS
# ------------------------------------
raw_folder = r"C:\Users\Sakshi\OneDrive\Desktop\SQL_Project\Data\raw_unprocessed_Data\output_by_month"
processed_folder = r"C:\Users\Sakshi\OneDrive\Desktop\SQL_Project\Data\Processed_Data"

# ------------------------------------
# DB CONNECTION
# ------------------------------------
def db_connection():
    conn = sc.connect(
        host='localhost',
        user='root',
        password='Root@1234',
        database='sakshi_project',
        port=3306
    )
    cursor = conn.cursor()
    return cursor, conn

# ------------------------------------
# ALTER PHONE COLUMN
# ------------------------------------
def alter_phone_column(table_name):
    cursor, conn = db_connection()
    try:
        cursor.execute(f"ALTER TABLE {table_name} MODIFY COLUMN phone VARCHAR(50)")
        conn.commit()
    except:
        pass
    cursor.close()
    conn.close()

# ------------------------------------
# LOAD FILE FUNCTION
# ------------------------------------
def load_files(table_name, file_path):

    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return

    # Only filename (important)
    only_filename = os.path.basename(file_path)

    cursor, conn = db_connection()

    # Fix phone column if exists
    alter_phone_column(table_name)

    # DELETE old records for this filename
    delete_sql = f"DELETE FROM {table_name} WHERE filename = %s"
    cursor.execute(delete_sql, (only_filename,))
    conn.commit()

    # READ FILE
    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path)
    elif file_path.endswith(".txt"):
        df = pd.read_csv(file_path, delimiter="|")
    elif file_path.endswith(".json"):
        df = pd.read_json(file_path)
    elif file_path.endswith(".xlsx"):
        df = pd.read_excel(file_path)
    else:
        print(f"❌ Unsupported format: {file_path}")
        return

    # ADD EXTRA COLUMNS
    df["filename"] = only_filename
    df["user_name"] = "sakshi"
    df["rownumber"] = range(1, len(df) + 1)

    # Prepare INSERT
    columns = df.columns.tolist()
    column_str = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))

    insert_sql = f"""
        INSERT INTO {table_name} ({column_str})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE Last_updated_at = CURRENT_TIMESTAMP()
    """

    cursor.executemany(insert_sql, df.values.tolist())
    conn.commit()

    # Update timestamp for all rows
    update_sql = f"""
        UPDATE {table_name}
        SET Last_updated_at = CURRENT_TIMESTAMP()
        WHERE filename = %s
    """
    cursor.execute(update_sql, (only_filename,))
    conn.commit()

    print(f"✔ Loaded: {only_filename} → {table_name}")

    # MOVE TO PROCESSED FOLDER
    shutil.move(file_path, os.path.join(processed_folder, only_filename))
    print("📦 File moved to Processed folder!")

    cursor.close()
    conn.close()

# ------------------------------------
# AUTO DETECT RAW FILES
# ------------------------------------
all_files = glob.glob(raw_folder + "/*")

# ------------------------------------
# TABLE MAPPING (BASED ON FILE NAME)
# ------------------------------------
table_mapping = {
    "orders": "ecommerce_orders_raw_table3",
    "crm": "crm_cutomer_raw_table4",
    "marketing": "marketing_events_raw_table4",
    "support": "support_tickets_raw_table4"
}

# ------------------------------------
# PROCESS EACH FILE AUTOMATICALLY
# ------------------------------------
for file_path in all_files:
    file_name = os.path.basename(file_path).lower()
    selected_table = None

    # Identify table using keyword
    for key, table in table_mapping.items():
        if key in file_name:
            selected_table = table
            break

    if selected_table:
        load_files(selected_table, file_path)
    else:
        print(f"⚠ No table found for: {file_path}")
