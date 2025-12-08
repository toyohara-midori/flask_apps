import csv
import io
import uuid
from datetime import datetime, date, timedelta

# 同階層の db_connection をインポート
from .db_connection import get_connection
from .Get_DB_Time import get_db_server_time

TARGET_DB = "master"

# ---------------------------------------------------------
# 設定: モードごとのテーブルと稼働時間
# ---------------------------------------------------------
MODE_CONFIG = {
    'normal': {
        'name': '通常予約',       # 夜締め予約発注
        'table': 'DBA.hacflr',   # ★夜は hacflr
        'start': '08:00',
        'end':   '20:00'
    },
    'morning': {
        'name': '当日朝締め',     # 朝締め発注
        'table': 'DBA.hacfl04',  # ★朝は hacfl04
        'start': '05:00',
        'end':   '10:50'
    }
}

def check_time_and_get_config(mode):
    """
    モードに応じた設定を返しつつ、稼働時間チェックを行う
    （Get_DB_Time を使ってDB時間で判定）
    """
    config = MODE_CONFIG.get(mode)
    if not config:
        return False, "不正なモードです。", None

    # ★汎用部品を使ってDB時間を取得
    now = get_db_server_time()
    current_time = now.time()
    
    # 時間チェック (Start <= Current <= End)
    start_dt = datetime.strptime(config['start'], "%H:%M").time()
    end_dt   = datetime.strptime(config['end'], "%H:%M").time()
    
    # ★開発中に時間制限を無効にしたい場合はここをコメントアウト
    if not (start_dt <= current_time <= end_dt):
        return False, f"{config['name']} の受付時間外です ({config['start']}～{config['end']})", None
        
    return True, "", config


# =========================================================
#  単発登録用ロジック
# =========================================================
def insert_single_record(mode, form_data):
    """
    画面からの単発入力を検証して登録する
    """
    # 1. 時間＆モードチェック
    is_ok, msg, config = check_time_and_get_config(mode)
    if not is_ok: return False, msg

    # 2. 値取得
    cucd = form_data.get('cucd', '').strip()
    cocd = form_data.get('cocd', '').strip()
    odsu = form_data.get('odsu', '').strip()
    
    oddt = form_data.get('oddt', '').strip()
    if not oddt: oddt = None
    
    dldt = form_data.get('dldt', '').strip()
    if not dldt: dldt = None

    # 3. 簡易バリデーション
    errors = []
    if len(cucd) != 3: errors.append("店舗CDは3桁必須")
    if len(cocd) != 8: errors.append("商品CDは8桁必須")
    if not odsu.isdigit() or int(odsu) == 0: errors.append("発注数は1以上の数値")
    
    # ★追加: 発注日(oddt)のモード別チェック
    if oddt:
        try:
            input_date = datetime.strptime(oddt, '%Y-%m-%d').date()
            # DB時間から「今日」を取得
            now = get_db_server_time()
            today = now.date()

            if input_date < today:
                errors.append(f"発注日に過去の日付は指定できません({oddt})")
            
            # 朝締めは「当日」以外不可
            if mode == 'morning' and input_date != today:
                errors.append(f"当日朝締めでは、当日以外の発注日は指定できません")
                
        except ValueError:
            errors.append("発注日の形式が不正です")

    if errors: return False, " / ".join(errors)

    # 4. 登録処理 (マスタから部門などを補完してINSERT)
    conn = None
    try:
        conn = get_connection(TARGET_DB)
        cursor = conn.cursor()
        
        target_table = config['table']
        
        sql = f"""
            INSERT INTO {target_table} 
            (edpno, type, cucd, cocd, bucd, odsu, oddt, dldt)
            SELECT 
                NULL,
                '004',
                ?,
                ?,
                (SELECT bucd FROM DBA.comf204 WHERE cocd = ?), -- マスタから部門取得
                ?,
                ?,
                ?
        """
        # cocdは2回渡す(INSERT値用とSELECT条件用)
        params = [cucd, cocd, cocd, odsu, oddt, dldt]
        
        cursor.execute(sql, params)
        conn.commit()
        
        return True, f"店舗:{cucd} 商品:{cocd} を{config['name']}で登録しました。"

    except Exception as e:
        if conn: conn.rollback()
        return False, f"DBエラー: {str(e)}"
    finally:
        if conn: conn.close()


# =========================================================
#  CSV一括登録用ロジック
# =========================================================
def parse_and_insert_work(file_storage, mode):
    """
    CSVを読み込み、ワークテーブルに登録する。
    """
    # 1. 時間チェック
    is_ok, msg, config = check_time_and_get_config(mode)
    if not is_ok: return False, msg, None

    # 2. 事前チェック (ファイル)
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

    # ヘッダー判定 (3列目の 'odsu' が数字でなければヘッダーとみなす)
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

        # インサートSQL (5項目 + 行番号)
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
            
            # --- 値取得 & 桁数チェック ---
            cucd_val = row[0].strip()
            if len(cucd_val) > 3:
                return False, f"{i}行目: 店舗CDが長すぎます('{cucd_val}')", None

            cocd_val = row[1].strip()
            if len(cocd_val) > 8:
                return False, f"{i}行目: 商品CDが長すぎます('{cocd_val}')", None

            # CSV内重複チェック
            current_key = (cucd_val, cocd_val)
            if current_key in seen_keys:
                return False, f"{i}行目: 店舗CD '{cucd_val}' 商品CD '{cocd_val}' が重複しています。", None
            seen_keys.add(current_key)

            # --- 値変換 ---
            odsu_str = row[2].strip()
            odsu_val = int(odsu_str) if odsu_str.isdigit() else 0

            oddt_val = clean_val(row[3])
            dldt_val = clean_val(row[4])

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


def exec_db_validation(cursor, batch_id, mode):
    """ SQLによる一括チェック """
    
    # DB時間を基準にする
    now = get_db_server_time()
    today_str = now.strftime('%Y-%m-%d')
    
    # 納品日の上限など
    limit_date = now.date() + timedelta(days=60)
    limit_str = limit_date.strftime('%Y-%m-%d')

    # -------------------------------------------------
    # 共通チェック
    # -------------------------------------------------
    # マスタチェック
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [店舗マスタ未登録]' WHERE batch_id = ? AND cucd NOT IN (SELECT cucd FROM DBA.cusmf04)", (batch_id,))
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [comf1未登録]' WHERE batch_id = ? AND cocd NOT IN (SELECT cocd FROM DBA.comf1)", (batch_id,))
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [comf204未登録]' WHERE batch_id = ? AND cocd NOT IN (SELECT cocd FROM DBA.comf204)", (batch_id,))
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [comf3未登録]' WHERE batch_id = ? AND cocd NOT IN (SELECT cocd FROM DBA.comf3)", (batch_id,))

    # 値チェック
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [店舗CDスペース不可]' WHERE batch_id = ? AND cucd LIKE '% %'", (batch_id,))
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [D店舗不可]' WHERE batch_id = ? AND cucd LIKE 'D%'", (batch_id,))
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [発注数0]' WHERE batch_id = ? AND odsu = 0", (batch_id,))

    # 納品日チェック
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [納品日が過去]' WHERE batch_id = ? AND dldt IS NOT NULL AND dldt < CAST(? AS DATE)", (batch_id, today_str))
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [納品日が2ヶ月先]' WHERE batch_id = ? AND dldt IS NOT NULL AND dldt > CAST(? AS DATE)", (batch_id, limit_str))

    # -------------------------------------------------
    # ★追加: 発注日(oddt)のモード別チェック
    # -------------------------------------------------
    
    # 共通: 過去日はNG (前日以前)
    cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [発注日が過去]' WHERE batch_id = ? AND oddt IS NOT NULL AND oddt < CAST(? AS DATE)", (batch_id, today_str))

    # 朝締め(morning)の場合: 「当日以外」はすべてNG（つまり未来もNG）
    if mode == 'morning':
        cursor.execute("UPDATE DBA.hacfl04_work SET err_msg = err_msg || ' [当日以外不可]' WHERE batch_id = ? AND oddt IS NOT NULL AND oddt <> CAST(? AS DATE)", (batch_id, today_str))


def get_work_data_checked(batch_id, mode):
    """ 
    バリデーション結果取得 
    ★変更: 引数に mode を追加 (バリデーションロジックの分岐のため)
    """
    conn = None
    data_list = []
    has_global_error = False
    
    if not batch_id: return True, []

    try:
        conn = get_connection(TARGET_DB)
        cursor = conn.cursor()
        
        # ★変更: 引数 mode を渡す
        exec_db_validation(cursor, batch_id, mode)
        conn.commit()

        # 行番号(line_num)順に取得
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
            line_num = row[0]
            db_err_msg = row[6] if row[6] else ""
            row_errors = []
            if db_err_msg: row_errors.append(db_err_msg)

            # 全角チェック
            all_text = "".join([str(col) for col in row[1:6] if col is not None])
            if not all(ord(c) < 128 for c in all_text):
                 row_errors.append(" [全角文字が含まれています]")

            if row_errors: has_global_error = True

            data_list.append({
                "line_num": line_num,
                "cols": row[1:6],
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


def migrate_work_to_main(batch_id, mode):
    """
    【CSV本登録用】
    modeを受け取り、INSERT先のテーブルを切り替える
    """
    # 1. 時間＆モードチェック (これでテーブル名も決まる)
    is_ok, msg, config = check_time_and_get_config(mode)
    if not is_ok: return False, msg, 0

    conn = None
    if not batch_id: return False, "BatchIDエラー", 0
    try:
        conn = get_connection(TARGET_DB)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM DBA.hacfl04_work WHERE batch_id = ?", (batch_id,))
        count_row = cursor.fetchone()
        row_count = count_row[0] if count_row else 0
        
        # モードに応じたテーブル名 (hacflr or hacfl04)
        target_table = config['table']
        
        # マスタから部門を補完してINSERT
        # ※朝も夜もカラム構成は同じ前提
        sql_copy = f"""
            INSERT INTO {target_table} 
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


def get_store_name_by_cd(cucd):
    """ 店舗CDから店舗名を取得 """
    conn = None
    try:
        conn = get_connection(TARGET_DB)
        cursor = conn.cursor()
        
        sql = "SELECT nmkj FROM DBA.cusmf04 WHERE cucd = ?"
        cursor.execute(sql, (cucd,))
        row = cursor.fetchone()
        
        return row[0].strip() if row else None
    except:
        return None
    finally:
        if conn: conn.close()


def get_product_info_by_cd(cocd):
    """ 商品CDから詳細情報(品名, 規格, メーカー, 部門, 入数, B単)を取得 """
    conn = None
    try:
        conn = get_connection(TARGET_DB)
        cursor = conn.cursor()
        
        sql = """
            SELECT 
                c3.hnam_k, c3.kika_k, c3.mnam_p, -- 0,1,2
                c2.bucd, c2.irsu, c2.btan       -- 3,4,5
            FROM DBA.comf3 c3
            LEFT JOIN DBA.comf204 c2 ON c3.cocd = c2.cocd
            WHERE c3.cocd = ?
        """
        cursor.execute(sql, (cocd,))
        row = cursor.fetchone()
        
        if row:
            return {
                "name": row[0] or "",
                "kika": row[1] or "",
                "maker": row[2] or "",
                "bucd": row[3] or "",
                "irsu": row[4] if row[4] is not None else "",
                "btan": row[5] if row[5] is not None else ""
            }
        return None
    except:
        return None
    finally:
        if conn: conn.close()