from datetime import datetime
from .db_connection import get_connection

def get_db_server_time(target_db="master"):
    """
    DBサーバーの現在時刻を取得する汎用関数
    
    Args:
        target_db (str): 接続先DBのキー (デフォルト: "master")
    
    Returns:
        datetime: DBの現在時刻
                  (DB接続エラー等の場合は、フェイルセーフとしてWebサーバーの現在時刻を返す)
    """
    conn = None
    try:
        conn = get_connection(target_db)
        cursor = conn.cursor()
        
        # 一般的なSQL (Sybase, SQL Anywhere, SQL Server, PostgreSQL等で動作)
        cursor.execute("SELECT CURRENT_TIMESTAMP")
        row = cursor.fetchone()
        
        if row and row[0]:
            # DBから取得できた場合、その値を返す
            return row[0]
            
    except Exception as e:
        # ログに出力するなど（ここではprintで代用）
        print(f"[WARNING] DB時刻取得失敗: {str(e)} -- システム時刻を使用します")
        
    finally:
        if conn:
            conn.close()
    
    # 取得失敗時のフェイルセーフ：Webサーバーの現在時刻
    return datetime.now()