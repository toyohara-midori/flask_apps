"""
common/db_master_access.py
--------------------------
マスタ系DBアクセスロジック（店舗マスタなど）
autosupply_web / cart_stay_register から共通利用される。
"""
# from common.db_connection import get_connection

# --- 店舗CDチェック + 店舗名（nmkn）取得
# ---    戻り値: (ok: bool, msg: str, cucd_normalized: str|None, nmkj: str|None)
def chk_cucd(conn, cucd):
    if not cucd:
        return False, "店舗CDを入力してください。", None, None

    cucd = cucd.strip()
    if len(cucd) != 3:
        return False, "店舗CDは3桁で入力してください。", None, None

    #sql = "SELECT nmkj FROM Cusmf04 WHERE cucd = ? AND cukb = '0'"
    sql = f"""
            SELECT REPLACE(a.nmkj, 'ジェーソン', '') AS nmkj_trimmed 
             FROM DBA.cusmf04 a 
             WHERE a.cucd = ? AND a.cukb = '0'
             AND a.cucd NOT IN (
               SELECT cucd FROM DBA.closemf04 GROUP BY cucd);
            """
    cur = conn.cursor()
    cur.execute(sql, (cucd,))
    row = cur.fetchone()

    if not row:
        return False, "不正な店舗CDです", None, None

    nmkj = getattr(row, "nmkj_trimmed", row[0] if row else "")

    return True, "", cucd, nmkj  # ← (OK, メッセージ, 正規化CUCD, 取得した店舗名)

