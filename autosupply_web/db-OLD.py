#######################
#  DB接続ユーティリティ
#######################
import pyodbc

def get_connection():
    # SQL Anywhereへの接続を返す
    conn_str = (
        "DRIVER={SQL Anywhere 12};"
        "UID=dba;"
        "PWD=jsndba;"
        "DBN=master;"                       # ← 実DB名（本当に 'master' か要確認）
        "ENG=asantkikan01;"                 # ← SQL Anywhere サーバ（エンジン）名
        "LINKS=TCPIP(HOST=asantkikan01x64;PORT=2638);"  # ← ホスト/IP とポート
    )
    return pyodbc.connect(conn_str)
