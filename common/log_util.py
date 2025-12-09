from flask import request
from .db_connection import get_connection
from .Get_DB_Time import get_db_server_time

def get_client_ip():
    """ クライアントのIPアドレスを取得する (プロキシ/IIS対応版) """
    if not request:
        return '0.0.0.0'
    
    # X-Forwarded-Forヘッダーがあればそちらを優先 (IIS経由など)
    x_forwarded = request.headers.getlist("X-Forwarded-For")
    if x_forwarded:
        # 複数ある場合は先頭がクライアントIP
        return x_forwarded[0].split(',')[0].strip()
        
    return request.remote_addr or '0.0.0.0'

def write_op_log(user_id, module, action, msg):
    """
    操作ログを DBA.weblog テーブルに記録する
    """
    conn = None
    try:
        conn = get_connection("master") # ログ保存先DB
        cursor = conn.cursor()
        
        client_ip = get_client_ip()
        
        # 時間はDB時間を使用
        now = get_db_server_time()
        
        # ★変更: テーブル名を DBA.weblog にしました
        sql = """
            INSERT INTO DBA.weblog 
            (log_dt, user_id, client_ip, module, action, msg)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        
        cursor.execute(sql, (now, user_id, client_ip, module, action, msg))
        conn.commit()
        
    except Exception as e:
        # ログ書き込み失敗でメイン処理を止めないよう、エラーはコンソールに出すだけにする
        print(f"[Log Error] Failed to write log: {e}")
    finally:
        if conn: conn.close()