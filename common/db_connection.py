"""
common/db_connection.py
-----------------------
アプリ共通のDB接続モジュール。SQL Server / SQL Anywhere 両対応。
接続先を名前で指定して呼び出す。
"""

import pyodbc

# 各アプリ用のDB接続定義
DB_CONFIGS = {
    # SQL Anywhere
    "master": {
        "TYPE": "SQLAnywhere",
        "DRIVER": "{SQL Anywhere 12}",
        "UID": "dba",
        "PWD": "jsndba",
        "DBN": "master",
        "ENG": "asantkikan01",
        "LINKS": "TCPIP(HOST=asantkikan01x64;PORT=2638)"
    },

    # SQL Server
    # インスタンスがない場合は、空白で設定すること
    # インスタンスの代わりにポート指定をしたい場合は、
    #   "SERVER": "SQLS08-14,1433"
    # のように設定すること(INSTANCEは空白にする)
    "tenposeisan": {
        "TYPE": "SQLServer",
        "SERVER": "DB-dataag1",
        "INSTANCE": "TENPOSEISAN",
        "DATABASE": "tenpo_seisan",
        "USER": "tenpo",
        "PASSWORD": "tenpo3080"
    },
    "SQLS08-14": {
        "TYPE": "SQLServer",
        "SERVER": "SQLS08-14",
        "INSTANCE": "",
        "DATABASE": "JSNDWH-b",
        "USER": "sqlsadmin",
        "PASSWORD": "Jason3080"
    }
}

def get_connection(db_key: str):
    """
    DB接続を取得。
    db_key: "master", "tenposeisan" など
    """
    if db_key not in DB_CONFIGS:
        raise ValueError(f"Unknown DB key: {db_key}")

    cfg = DB_CONFIGS[db_key]
    db_type = cfg.get("TYPE", "SQLServer")

    if db_type == "SQLAnywhere":
        # SQL Anywhere接続文字列
        conn_str = (
            f"DRIVER={cfg['DRIVER']};"
            f"UID={cfg['UID']};"
            f"PWD={cfg['PWD']};"
            f"DBN={cfg['DBN']};"
            f"ENG={cfg['ENG']};"
            f"LINKS={cfg['LINKS']};"
        )

    elif db_type == "SQLServer":
        # サーバー指定（インスタンスがある場合のみ結合）
        if cfg.get("INSTANCE"):
            server_str = f"{cfg['SERVER']}\\{cfg['INSTANCE']}"
        else:
            server_str = cfg["SERVER"]  # ← インスタンスなし

        # SQL Server接続文字列
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server_str};"
            f"DATABASE={cfg['DATABASE']};"
            f"UID={cfg['USER']};"
            f"PWD={cfg['PASSWORD']};"
            "TrustServerCertificate=yes;"
        )

    else:
        raise ValueError(f"Unsupported DB type: {db_type}")

    return pyodbc.connect(conn_str)
