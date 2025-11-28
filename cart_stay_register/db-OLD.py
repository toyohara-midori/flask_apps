"""
cart_stay_register/db.py
------------------------
滞留カゴ車登録用 データベース接続定義（SQL Server）
"""

import pyodbc

# SQL Server接続設定
DB_SERVER = "DB-dataag1"
DB_INSTANCE = "TENPOSEISAN"
DB_NAME = "tenpo"
DB_USER = "tenpo"
DB_PASSWORD = "tenpo3080"

# 接続文字列を組み立て
CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={DB_SERVER}\\{DB_INSTANCE};"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
    "TrustServerCertificate=yes;"
)


def get_connection():
    """
    SQL Serverへの接続を返す。
    利用例:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM ...")
            rows = cur.fetchall()
    """
    return pyodbc.connect(CONN_STR)