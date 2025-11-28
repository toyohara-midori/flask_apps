#######################
#  業務ロジック系処理
#######################
import pyodbc
from common.db_connection import get_connection
from .config_util import get_arsjy04_table
from datetime import datetime


# --- 什器番号チェック ---
def chk_jyno(jyno):
    if not jyno:
        return False, "什器番号を入力してください。"

    if not jyno.isdigit():
        return False, "什器番号には数字のみを入力してください。"

    if len(jyno) != 5:
        return False, "什器番号の桁数が違います。"

    if not all(ch.isdigit() for ch in jyno):
        return False, "什器番号の入力に誤りがあります。"

    return True, "OK"

# --- 表示ボタン押下時 発注曜日ロード（'o'→True、空白/None→False） ---
def load_odflg(conn, typeflg: str, cucd: str, jyno: str):
    tbl = get_arsjy04_table()
    sql = f"SELECT sun, mon, tue, wed, thu, fri, sat FROM {tbl} WHERE cucd = ? AND jyno = ?"
    cur = conn.cursor()
    cur.execute(sql, (cucd, jyno))
    row = cur.fetchone()
    cols = ("sun","mon","tue","wed","thu","fri","sat")

    if not row:
        return False, {c: False for c in cols}

    def _is_checked(v):
        if v is None:
            return False
        if isinstance(v, bytes):
            try:
                v = v.decode(errors="ignore")
            except Exception:
                v = str(v)
        s = str(v).strip().lower()
        return s == 'o'

    days = {c: _is_checked(row[i]) for i, c in enumerate(cols)}
    return True, days

# --- 登録ボタン押下時 新規登録 ---
def insert_record(cucd: str, jyno: str, days: dict, conn=None):

    #print("[insert_record] args", 
    #  "cucd=", repr(cucd), "jyno=", repr(jyno), 
    #  "days=", repr(days), "conn_is_None=", conn is None, flush=True)
    
    tbl = get_arsjy04_table()
    now = datetime.now()
    ti = now.strftime("%H:%M:%S")
    dt = now.strftime("%Y-%m-%d")
    type_val = "004"

    # '1'→'o' 変換
    def v1(x):
        s = str(x).strip().lower()
        return 'o' if (x is True) or (s in ('1','on','true','t','yes','y')) else ''
    week_vals = [v1(days.get(c)) for c in ["sun","mon","tue","wed","thu","fri","sat"]]

    outer_conn = conn is not None
    try:
        if not outer_conn:
            conn = get_connection()
        cur = conn.cursor()

        # 既存チェック（必要なら TRIM(type)=? も条件に追加）
        cur.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE cucd=? AND jyno=?",
            (cucd.strip(), jyno.strip())
        )

        cnt = cur.fetchone()[0] or 0

        # print("[insert_record] exists cnt:", cnt, flush=True)

        if cnt > 0:

            # --- ★ 既存データを取得して比較 ---
            cur.execute(
                f"SELECT sun, mon, tue, wed, thu, fri, sat FROM {tbl} WHERE cucd=? AND jyno=?",
                (cucd.strip(), jyno.strip())
            )
            row = cur.fetchone()

            # 'o'／空白を統一して比較
            existing = [str(r or '').strip().lower() for r in row]
            new_vals = [v.lower() for v in week_vals]

            if existing == new_vals:
                # 全項目一致 → 更新せずリターン
                return False, "変更がないため更新しませんでした。"

            # --- 差分あり → UPDATE実行 ---
            sql = f"""
                UPDATE {tbl} SET sun=?, mon=?, tue=?, wed=?, thu=?, fri=?, sat=?, 
                upti=?, updt=? 
                WHERE cucd=? AND jyno=?
            """
            vals = [*week_vals, ti, dt, cucd, jyno]
        else:
            # INSERT（13項目）
            sql = f"""
                INSERT INTO {tbl}
                  (type, cucd, jyno, sun, mon, tue, wed, thu, fri, sat, upti, updt, rgdt)
                VALUES
                  (?,    ?,    ?,    ?,   ?,   ?,   ?,   ?,   ?,   ?,   ?,    ?,    ?)
            """
            vals = [type_val, cucd, jyno, *week_vals, ti, dt, dt]

        #print("sql:", sql, " vals:", vals)
        cur.execute(sql, vals)
        conn.commit()
        
        return True, "登録完了しました。"

    except Exception as e:
        try:
            if conn: conn.rollback()
        except Exception:
            pass
        return False, f"登録中にエラーが発生しました: {e}"

    finally:
        if not outer_conn and conn:
            try: conn.close()
            except Exception: pass