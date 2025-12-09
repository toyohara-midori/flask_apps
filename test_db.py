# test_db.py
from common.db_connection import get_connection

def check_data():
    print("--- DB接続テスト開始 ---")
    try:
        # master (SQL Anywhere) に接続
        conn = get_connection('master')
        cursor = conn.cursor()
        print("1. DB接続成功")

        # DCNYU02 の件数確認
        cursor.execute("SELECT count(*) FROM DBA.DCNYU02")
        count02 = cursor.fetchone()[0]
        print(f"2. DBA.DCNYU02 の件数: {count02} 件")

        # DCNYU03 の件数確認
        cursor.execute("SELECT count(*) FROM DBA.DCNYU03")
        count03 = cursor.fetchone()[0]
        print(f"3. DBA.DCNYU03 の件数: {count03} 件")

        if count02 > 0 or count03 > 0:
            print("   -> データは存在します。")
            
            # 中身を1件だけ見てみる
            sql = "SELECT deno, dldt, cucd FROM DBA.DCNYU02 UNION ALL SELECT deno, dldt, cucd FROM DBA.DCNYU03"
            cursor.execute(sql)
            row = cursor.fetchone()
            print(f"4. 取得データサンプル: {row}")
        else:
            print("   -> データが0件です。INSERT後に COMMIT したか確認してください。")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"!!! エラー発生 !!!\n{e}")

if __name__ == "__main__":
    check_data()