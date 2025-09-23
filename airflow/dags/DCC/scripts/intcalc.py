import psycopg2
import datetime
import pandas as pd
from psycopg2 import sql
from sqlalchemy import create_engine
from zowe.zos_files_for_zowe_sdk import Files,DatasetOption
# Constants for database connection

DB_USER = 'postgres'

DB_PASSWORD = 'admin'

DB_HOST = 'localhost'

DB_PORT = '5432'

DB_DATABASE = 'postgres'

profile = {
    "host": "192.86.32.87",
    "port": "10443",
    "user": "cgdevds",
    "password": "capgem16"
}

# Initialize Zowe and Files object
zos_files = Files(profile)

#TRANSACT = 'AWS.M2.CARDDEMO.SYSTRAN.{}.dat'.format(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))


def connect_db():
    """Establish a database connection and return the connection object."""

    try:

        conn = psycopg2.connect(

            user=DB_USER,

            password=DB_PASSWORD,

            host=DB_HOST,

            port=DB_PORT,

            database=DB_DATABASE

        )

        return conn

    except Exception as e:

        print("Error connecting to the database:", e)

        return None


def calculate_interest(balance, rate=0.05):
    """Calculates interest based on the balance and a given rate."""

    return balance * rate


def calculate_fees(transaction_count, fee_per_transaction=0.5, threshold=5):
    """Calculates fees based on the number of transactions and a threshold."""

    if transaction_count > threshold:

        return (transaction_count - threshold) * fee_per_transaction

    else:

        return 0


def process_transaction_balance():
    print("Processing Transaction Balance from PostgreSQL tables...")

    conn = connect_db()

    if not conn:
        return

    try:

        cursor = conn.cursor()
        cursor.execute("""DROP TABLE IF EXISTS DISCGRP,CARDXREF,ACCTDATA1,TCATBALF1""")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS DISCGRP (
                dis_acct_group_id CHAR(10),
                dis_tran_type_cd CHAR(2),
                dis_tran_cat_cd INTEGER,
                dis_int_rate NUMERIC(6, 2),
                filler CHAR(28)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CARDXREF (
                xref_card_num CHAR(16),
                xref_cust_id BIGINT,
                xref_acct_id BIGINT,
                filler CHAR(14)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ACCTDATA1 (
                acct_id BIGINT,
                acct_active_status CHAR(1),
                acct_curr_bal DECIMAL(12, 2),
                acct_credit_limit DECIMAL(12, 2),
                acct_cash_credit_limit DECIMAL(12, 2),
                acct_open_date CHAR(10),
                acct_expiration_date CHAR(10),
                acct_reissue_date CHAR(10),
                acct_curr_cyc_credit DECIMAL(12, 2),
                acct_curr_cyc_debit DECIMAL(12, 2),
                acct_addr_zip CHAR(10),
                acct_group_id CHAR(10),
                filler CHAR(178)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TCATBALF1 (
                trancat_acct_id BIGINT,
                trancat_type_cd CHAR(2),
                trancat_cd INTEGER,
                tran_cat_bal NUMERIC(11,2),
                filler CHAR(22)
            )
        """)
        zos_files.ds.download("AWS.M5.CARDDEMO.DISCGRP.VSAM.KSDS","AWS.M5.CARDDEMO.DISCGRP.VSAM.KSDS.txt")
        zos_files.ds.download("AWS.M5.CARDDEMO.CARDXREF.VSAM.KSDS", "AWS.M5.CARDDEMO.CARDXREF.VSAM.KSDS.txt")
        zos_files.ds.download("AWS.M5.CARDDEMO.ACCTDATA.VSAM.KSDS", "AWS.M5.CARDDEMO.ACCTDATA.VSAM.KSDS.txt")
        zos_files.ds.download("AWS.M5.CARDDEMO.TCATBALF.VSAM.KSDS", "AWS.M5.CARDDEMO.TCATBALF.VSAM.KSDS.txt")
        with open("AWS.M5.CARDDEMO.DISCGRP.VSAM.KSDS.txt", "r") as file:
            for line in file:
                # Parse the line into fields based on fixed widths
                dis_acct_group_id = line[0:10].strip()
                dis_tran_type_cd = line[10:12].strip()
                dis_tran_cat_cd = int(line[12:16].strip())
                dis_int_rate = float(
                    line[16:22].replace('{', '').replace('}', '').strip()) / 100  # Assuming implied decimal
                filler = line[22:50].strip()

                # Insert data into PostgreSQL
                cursor.execute("""
                    INSERT INTO DISCGRP (dis_acct_group_id, dis_tran_type_cd, dis_tran_cat_cd, dis_int_rate, filler)
                    VALUES (%s, %s, %s, %s, %s)
                """, (dis_acct_group_id, dis_tran_type_cd, dis_tran_cat_cd, dis_int_rate, filler))
        with open("AWS.M5.CARDDEMO.CARDXREF.VSAM.KSDS.txt", "r") as file:
            for line in file:
                # Parse the line into fields based on fixed widths
                xref_card_num = line[0:16].strip()
                xref_cust_id = int(line[16:25])
                xref_acct_id = int(line[25:36])
                filler = line[36:50].strip()  # This captures the filler space

                # Insert data into PostgreSQL
                cursor.execute("""
                    INSERT INTO CARDXREF (xref_card_num, xref_cust_id, xref_acct_id, filler)
                    VALUES (%s, %s, %s, %s)
                """, (xref_card_num, xref_cust_id, xref_acct_id, filler))
        with open("AWS.M5.CARDDEMO.ACCTDATA.VSAM.KSDS.txt",'r') as file:
            for line in file:
                # Extract fields based on fixed positions and lengths
                acct_id = int(line[0:11].strip())  # PIC 9(11)
                acct_active_status = line[11:12].strip()  # PIC X(01)
                acct_curr_bal = float(line[12:24].strip().replace('{', '').replace('}', '-')) / 100  # PIC S9(10)V99
                acct_credit_limit = float(line[24:36].strip().replace('{', '').replace('}', '-')) / 100  # PIC S9(10)V99
                acct_cash_credit_limit = float(
                    line[36:48].strip().replace('{', '').replace('}', '-')) / 100  # PIC S9(10)V99
                acct_open_date = line[48:58].strip()  # PIC X(10)
                acct_expiration_date = line[58:68].strip()  # PIC X(10)
                acct_reissue_date = line[68:78].strip()  # PIC X(10)
                acct_curr_cyc_credit = float(
                    line[78:90].strip().replace('{', '').replace('}', '-')) / 100  # PIC S9(10)V99
                acct_curr_cyc_debit = float(
                    line[90:102].strip().replace('{', '').replace('}', '-')) / 100  # PIC S9(10)V99
                acct_addr_zip = line[102:112].strip()  # PIC X(10)
                acct_group_id = line[112:122].strip()  # PIC X(10)
                filler = line[122:300].strip()  # PIC X(178)

                # Insert data into PostgreSQL
                insert_query = """
                INSERT INTO ACCTDATA1 (
                    acct_id, acct_active_status, acct_curr_bal, acct_credit_limit, acct_cash_credit_limit,
                    acct_open_date, acct_expiration_date, acct_reissue_date, acct_curr_cyc_credit,
                    acct_curr_cyc_debit, acct_addr_zip, acct_group_id, filler
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_query, (
                    acct_id, acct_active_status, acct_curr_bal, acct_credit_limit, acct_cash_credit_limit,
                    acct_open_date, acct_expiration_date, acct_reissue_date, acct_curr_cyc_credit,
                    acct_curr_cyc_debit, acct_addr_zip, acct_group_id, filler
                ))
        with open("AWS.M5.CARDDEMO.TCATBALF.VSAM.KSDS.txt", 'r') as file:
            for line in file:
                # Extract fields based on fixed width
                trancat_acct_id = int(line[0:11])
                trancat_type_cd = line[11:13]
                trancat_cd = int(line[13:17])

                # Convert the signed numeric value
                tran_cat_bal = float(line[17:28].replace('{', '0').replace('}', '0')) / 100

                filler = line[28:50]

                # Insert data into PostgreSQL
                cursor.execute("""
                    INSERT INTO TCATBALF1 (
                        trancat_acct_id, trancat_type_cd, trancat_cd, tran_cat_bal, filler
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (
                    trancat_acct_id, trancat_type_cd, trancat_cd, tran_cat_bal, filler
                ))
        conn.commit()
        query = """

        SELECT CONCAT(TO_CHAR(CURRENT_DATE, 'YYYYMMDD'),t.trancat_acct_id) AS TRAN_ID,'01' AS tran_type_cd,'05' AS TRAN_CAT_CD, 'System' AS TRAN_SOURCE, 'Int. for a/c '||a.acct_id AS TRAN_DESC, COALESCE(d.dis_int_rate, 0) as INT_RATE,'0' AS TRAN_MERCHANT_ID, '     ' AS TRAN_MERCHANT_NAME,'     ' AS TRAN_MERCHANT_CITY,'    ' AS TRAN_MERCHANT_ZIP,c.xref_card_num AS TRAN_CARD_NUM,TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF6') AS TRAN_ORIG_TS,TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.FF6') AS TRAN_PROC_TS,a.acct_curr_bal

        FROM "tcatbalf1" t

        LEFT JOIN "acctdata1" a ON t.trancat_acct_id = a.acct_id

        LEFT JOIN "discgrp" d ON a.acct_group_id = d.dis_acct_group_id

		LEFT JOIN "cardxref" c ON t.trancat_acct_id= c.xref_acct_id

        """

        cursor.execute(query)

        records = cursor.fetchall()
        recordlist=[]
        for record in records:
            # print(record)
            balance = record[13]
            interest_rate = record[5]
            interest = calculate_interest(balance, rate=interest_rate)
            y = list(record)
            y[5] = interest
            y=y[:-1]
            record=tuple(y)
            recordlist.append(record)
        diff_df = pd.DataFrame(recordlist, columns=[
            'TRAN-ID', 'TRAN-TYPE-CD', 'TRAN-CAT-CD', 'TRAN-SOURCE', 'TRAN-DESC',
            'TRAN_AMT', 'TRAN_MERCHANT_ID', 'TRAN_MERCHANT_NAME', 'TRAN_MERCHANT_CITY',
            'TRAN_MERCHANT_ZIP', 'TRAN_CARD_NUM', 'TRAN_ORIG_TS', 'TRAN_PROC_TS'
        ])
        conn_string = "postgresql://postgres:admin@127.0.0.1:5432/postgres"
        db = create_engine(conn_string)
        conn = db.connect()
        diff_df.to_sql('TRANSACT', con=conn, if_exists='replace', index=False)
    except Exception as e:

        print("Error processing transaction balance:", e)

    finally:

        cursor.close()

        conn.close()


if __name__ == "__main__":
    process_transaction_balance()