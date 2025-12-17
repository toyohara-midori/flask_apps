# common/logger.py
from flask import request
import datetime

try:
    from common.db_connection import get_connection
except ImportError:
    # 簡易的なフォールバック（環境に合わせて直してください）
    from common.db_connection import get_connection

def write_log(module_name, user_id, action_type, message):
    """
    weblogテーブルにログを書き込む共通関数
    """
    conn = None
    cursor = None
    try:
        # IPアドレスの取得 (プロキシ経由の場合は X-Forwarded-For を見る等の調整が必要ですが一旦これで行きます)
        ip = request.remote_addr if request else 'unknown'
        
        # DB接続
        conn = get_connection('master') # ログはmasterに入れる想定
        cursor = conn.cursor()

        sql = """
            INSERT INTO weblog (
                log_date, module_name, user_id, action_type, message, ip_address
            ) VALUES (
                CURRENT_TIMESTAMP, ?, ?, ?, ?, ?
            )
        """
        
        cursor.execute(sql, [module_name, user_id, action_type, message, ip])
        conn.commit()
        
        # 開発用コンソール出力
        print(f"[LOG] {module_name} | {user_id} | {action_type} | {message}")

    except Exception as e:
        # ログ書き込みエラーで本処理を止めない
        print(f"Log Write Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()