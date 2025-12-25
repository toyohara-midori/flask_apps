# common/logger.py
from flask import request

# インポートパスの調整（環境依存を防ぐためtry-except）
try:
    from common.db_connection import get_connection
except ImportError:
    from .db_connection import get_connection

def get_client_ip():
    """
    クライアントのIPアドレスを取得する (プロキシ/IIS対応版)
    """
    if not request:
        return '0.0.0.0'
    
    # IISやロードバランサ経由の場合、X-Forwarded-Forを優先
    x_forwarded = request.headers.getlist("X-Forwarded-For")
    if x_forwarded:
        return x_forwarded[0].split(',')[0].strip()
        
    return request.remote_addr or '0.0.0.0'

def write_log(module_name, user_id, action_type, message):
    """
    ログを書き込む共通関数
    呼び出し元の修正を不要にするため、関数名と引数は logger.py の形式を維持し、
    中身は正しいDB定義(DBA.weblog)に合わせています。
    """
    conn = None
    cursor = None
    try:
        ip = get_client_ip()
        conn = get_connection('master')
        cursor = conn.cursor()

        # ★ここを「B」の正しい定義(DBA.weblog / log_dt)に修正しました
        # 日時はSQL側で CURRENT_TIMESTAMP を入れればPython側での取得は不要です
        sql = """
            INSERT INTO DBA.weblog 
            (log_dt, user_id, client_ip, module, action, msg)
            VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)
        """
        
        # 引数のマッピング (logger.pyの引数 -> Bのテーブル定義)
        # module_name -> module
        # action_type -> action
        # message     -> msg
        cursor.execute(sql, [user_id, ip, module_name, action_type, message])
        conn.commit()
        
        # 開発用出力
        print(f"[LOG] {module_name} | {user_id} | {action_type} | {message}")

    except Exception as e:
        print(f"[Log Write Error] {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()