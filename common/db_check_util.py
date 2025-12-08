from .db_connection import get_connection

MAINTENANCE_HTML = """
<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <title>Service Unavailable</title>
</head>
<body style="text-align: center; padding-top: 50px; font-family: sans-serif; background-color: #f4f6f9;">
    <div style="background: white; padding: 40px; border-radius: 8px; display: inline-block; box-shadow: 0 4px 10px rgba(0,0,0,0.1);">
        <h1 style="color: #555;">ただいまメンテナンス中です</h1>
        <p style="color: #666;">データベースへの接続が確認できませんでした。</p>
        <p style="color: #666;">しばらく時間を置いてから再接続してください。</p>
    </div>
</body>
</html>
"""

def is_db_available(target_db="master", timeout_sec=3):
    """
    DBが利用可能か高速にチェックする関数
    
    Args:
        target_db (str): 接続先DB識別子
        timeout_sec (int): タイムアウト秒数 (デフォルト3秒)
                           ※バックアップ中などは応答がないため、短めに設定して即切る
    
    Returns:
        bool: 接続できれば True, できなければ False
    """
    conn = None
    try:
        # 接続取得 (接続自体が失敗する場合のチェック)
        # ※使用しているドライバによっては接続時にtimeoutを指定できる場合がありますが
        #   ここでは一般的なtry-exceptで捕捉します
        conn = get_connection(target_db)
        
        # 軽いクエリを投げて応答を確認
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        
        return True
        
    except Exception as e:
        print(f"[DB Check] 接続不可: {str(e)}")
        return False
        
    finally:
        if conn:
            conn.close()