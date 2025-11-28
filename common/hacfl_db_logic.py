import csv
import io
import uuid
from datetime import datetime, date, timedelta

from .db_connection import get_connection

TARGET_DB = "master"

def parse_and_insert_work(file_storage):
    """
    CSVを読み込み、ワークテーブルに登録する。
    ★修正: CSVの行番号(line_num)をDBに保存するように変更
    """
    # 1. 事前チェック
    filename = file_storage.filename.lower()
    if not filename.endswith('.csv'):
        return False, "拡張子が .csv のファイルのみアップロード可能です。", None

    file_storage.seek(0, 2)
    size = file_storage.tell()
    file_storage.seek(0)

    if size == 0: return False, "ファイルが空です。", None
    if size > 10 * 1024 * 1024: return False, "ファイルサイズオーバー(10MBまで)", None

    batch_id = str(uuid.uuid4())
    
    # 文字コード判別
    csv_text = ""
    raw_data = file_storage.stream.read()
    try:
        csv_text = raw_data.decode("utf-8")
    except UnicodeDecodeError:
        try:
            csv_text = raw_data.decode("cp932")
        except UnicodeDecodeError:
            return False, "文字コード判別不能(UTF-8/SJISのみ)", None

    try:
        stream = io.StringIO(csv_text, newline=None)
        csv_reader = csv.reader(stream)
        rows = list(csv_reader)
    except Exception as e:
        return False, f"CSV読込エラー: {str(e)}", None

    # ヘッダー判定
    start_line_num = 1
    if len(rows) > 0:
        first_row = rows[0]
        if len(first_row) >= 3:
            val_chk = first_row[2].strip()
            if not val_chk.isdigit():
                rows.pop(0)
                start_line_num = 2 

    if len(rows) == 0:
        return False, "データ行が含まれていません。", None

    conn = None
    try:
        conn = get_connection(TARGET_DB)
        cursor = conn.cursor()

        # お掃除
        sql_cleanup = "DELETE FROM DBA.hacfl04_work WHERE oddt < ?"
        cursor.execute(sql_cleanup, (date.today(),))

        # ★変更: line_num を追加
        sql = """
            INSERT INTO DBA.hacfl04_work 
            (batch_id, line_num, cucd, cocd, odsu, oddt, dldt, err_msg)
            VALUES (?, ?, ?, ?, ?, ?, ?, '')
        """
        
        insert_count = 0
        seen_keys = set()
        
        def clean_val(v):
            s = v.strip()
            if s == '(NULL)': return None
            return s if s else None

        for i, row in enumerate(rows, start=start_line_num):
            if len(row) < 5: row += [''] * (5 - len(row))
            
            cucd_val = row[0].strip()
            if len(cucd_val) > 3:
                return False, f"{i}行目: 店舗CDが長すぎます('{cucd_val}')", None

            cocd_val = row[1].strip()
            if len(cocd_val) > 8:
                return False, f"{i}行目: 商品CDが長すぎます('{cocd_val}')", None

            # 重複チェック
            current_key = (cucd_val, cocd_val)
            if current_key in seen_keys:
                return False, f"{i}行目: 店舗CD '{cucd_val}' 商品CD '{cocd_val}' が重複しています。", None
            seen_keys.add(current_key)

            odsu_str = row[2].strip()
            odsu_val = int(odsu_str) if odsu_str.isdigit() else 0

            oddt_val = clean_val(row[3])
            dldt_val = clean_val(row[4])

            # ★変更: パラメータに i (行番号) を追加
            params = [batch_id, i, cucd_val, cocd_val, odsu_val, oddt_val, dldt_val]
            cursor.execute(sql, params)
            insert_count += 1
            
        conn.commit()
        return True, f"{insert_count}件取り込み完了", batch_id

    except Exception as e:
        if conn: conn.rollback()
        return False, f"登録エラー: {str(e)}", None
    finally:
        if conn: conn.close()


def exec_db_validation(cursor, batch_id):
    """ SQLによる一括チェック """
    today_str = date.today().strftime('%Y-%m-%d')
    limit_date = date.today() + timedelta(days=60)
    limit_str = limit_date.strftime('%Y-%m-%d')

    # マスタチェック
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [店舗マスタ未登録]' WHERE batch_id = ? AND cucd NOT IN (SELECT cucd FROM DBA.cusmf04)", (batch_id,))
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [comf1未登録]' WHERE batch_id = ? AND cocd NOT IN (SELECT cocd FROM DBA.comf1)", (batch_id,))
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [comf204未登録]' WHERE batch_id = ? AND cocd NOT IN (SELECT cocd FROM DBA.comf204)", (batch_id,))
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [comf3未登録]' WHERE batch_id = ? AND cocd NOT IN (SELECT cocd FROM DBA.comf3)", (batch_id,))

    # 値チェック
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [店舗CDスペース不可]' WHERE batch_id = ? AND cucd LIKE '% %'", (batch_id,))
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [D店舗不可]' WHERE batch_id = ? AND cucd LIKE 'D%'", (batch_id,))
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [発注数0]' WHERE batch_id = ? AND odsu = 0", (batch_id,))

    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [納品日が過去]' WHERE batch_id = ? AND dldt IS NOT NULL AND dldt < CAST(? AS DATE)", (batch_id, today_str))
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [納品日が2ヶ月先]' WHERE batch_id = ? AND dldt IS NOT NULL AND dldt > CAST(? AS DATE)", (batch_id, limit_str))


def get_work_data_checked(batch_id):
    conn = None
    data_list = []
    has_global_error = False
    
    if not batch_id: return True, []

    try:
        conn = get_connection(TARGET_DB)
        cursor = conn.cursor()
        
        exec_db_validation(cursor, batch_id)
        conn.commit()

        # ★変更: line_num を取得し、ORDER BY w.line_num (行番号順) に変更
        sql = """
            SELECT 
                w.line_num, w.cucd, w.cocd, w.odsu, w.oddt, w.dldt, w.err_msg, 
                c3.hnam_k, c3.kika_k, c3.mnam_p,
                c2.bucd, c2.irsu, c2.btan
            FROM DBA.hacfl04_work w
            LEFT JOIN DBA.comf3   c3 ON w.cocd = c3.cocd
            LEFT JOIN DBA.comf204 c2 ON w.cocd = c2.cocd
            WHERE w.batch_id = ?
            ORDER BY w.line_num ASC
        """
        cursor.execute(sql, (batch_id,))
        rows = cursor.fetchall()
        
        for row in rows:
            # line_num は row[0]
            line_num = row[0]
            
            # 以降のインデックスが1つずつずれます
            # cucd=1, cocd=2, odsu=3, oddt=4, dldt=5, err=6
            db_err_msg = row[6] if row[6] else ""
            row_errors = []
            if db_err_msg: row_errors.append(db_err_msg)

            # 全角チェック (cucd~dldt: index 1~5)
            all_text = "".join([str(col) for col in row[1:6] if col is not None])
            if not all(ord(c) < 128 for c in all_text):
                 row_errors.append(" [全角文字が含まれています]")

            if row_errors: has_global_error = True

            data_list.append({
                "line_num": line_num,   # ★追加: これを表示に使う
                "cols": row[1:6],       # データ部分
                
                # index修正: 7以降
                "item_name": row[7] if row[7] else "(-)", 
                "kika":      row[8] if row[8] else "",    
                "maker":     row[9] if row[9] else "",    
                "master_bucd": row[10] if row[10] else "",  
                "irsu":      row[11] if row[11] is not None else "", 
                "btan":      row[12] if row[12] is not None else "", 
                "errors": row_errors
            })

    except Exception as e:
        print(f"Check Error: {e}")
        return True, []
    finally:
        if conn: conn.close()
        
    return has_global_error, data_list


def migrate_work_to_main(batch_id):
    conn = None
    if not batch_id: return False, "BatchIDエラー", 0
    try:
        conn = get_connection(TARGET_DB)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM DBA.hacfl04_work WHERE batch_id = ?", (batch_id,))
        count_row = cursor.fetchone()
        row_count = count_row[0] if count_row else 0
        
        sql_copy = """
            INSERT INTO DBA.hacfl04 
            (edpno, type, cucd, cocd, bucd, odsu, oddt, dldt)
            SELECT 
                NULL, '004', w.cucd, w.cocd, c2.bucd, w.odsu, w.oddt, w.dldt
            FROM DBA.hacfl04_work w
            LEFT JOIN DBA.comf204 c2 ON w.cocd = c2.cocd
            WHERE w.batch_id = ?
        """
        cursor.execute(sql_copy, (batch_id,))
        cursor.execute("DELETE FROM DBA.hacfl04_work WHERE batch_id = ?", (batch_id,))
        conn.commit()
        return True, "本登録完了", row_count
    except Exception as e:
        if conn: conn.rollback()
        return False, f"本登録エラー: {str(e)}", 0
    finally:
        if conn: conn.close()