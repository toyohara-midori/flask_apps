"""
common/cucd_logic.py
--------------------
店舗CD（cucd）関連の共通ロジック。
autosupply_web, cart_stay_register など複数アプリで利用できるよう、
Flask に依存しない純粋な処理関数として定義。
"""

from common.db_connection import get_connection
from common.db_master_access import chk_cucd

def get_cucd_list():
    """
    店舗CDリストを取得し、"123(○○店)" の形式で返す。
    """
    with get_connection("master") as conn:
        cur = conn.cursor()
        sql = """
            SELECT a.cucd, REPLACE(a.nmkj, 'ジェーソン', '') AS nmkj
            FROM Cusmf04 a
            WHERE a.cukb = '0'
            AND a.cucd NOT IN ( SELECT cucd FROM DBA.closemf04 GROUP BY cucd)
            ORDER BY cucd;
        """
        cur.execute(sql)
        rows = cur.fetchall()
    return [f"{r[0]}({(r[1] or '').strip()})" for r in rows]


def check_cucd(cucd: str):
    """
    店舗CDをチェックし、結果を辞書で返す。
    """
    cucd = (cucd or "").strip()
    with get_connection("master") as conn:
        ok, msg, cucd_n, nmkn = chk_cucd(conn, cucd)
        return {"ok": ok, "msg": msg, "cucd": cucd_n, "nmkn": nmkn}

def get_cucd_name(cucd: str) -> str:
    """
    店舗CDから店舗名を取得する関数
    Excel出力で使うつもりだったけど、get_cucd_list() を使うことにしたので、
    こちらは不使用。でも一応とっておく
    """
    query = """
        SELECT REPLACE(a.nmkj, 'ジェーソン', '') AS nmkj
        FROM Cusmf04 a
        WHERE a.cukb = '0' AND a.cucd = ?
    """
    with get_connection("master") as conn:
        cur = conn.cursor()
        cur.execute(query, (cucd,))
        row = cur.fetchone()

    return row[0] if row else ""

def get_cucd_master_tuple():
    """
    店舗CDと店舗名をタプル形式で返す高速処理向け関数。
    例: [("B01", "赤羽"), ("111", "草加"), ...]
    """
    with get_connection("master") as conn:
        cur = conn.cursor()

        sql = """
            SELECT a.cucd, REPLACE(a.nmkj, 'ジェーソン', '') AS nmkj
            FROM Cusmf04 a
            WHERE a.cukb = '0'
              AND a.cucd NOT IN (SELECT cucd FROM DBA.closemf04 GROUP BY cucd)
            ORDER BY cucd
        """
        cur.execute(sql)
        rows = cur.fetchall()

    data = [(str(r[0]).strip(), (r[1] or "").strip()) for r in rows]

    # ★ ソート：Bxx → 数字
    data_sorted = sorted(
        data,
        key=lambda x: (
            not x[0].startswith("B"),               # B店舗を先に
            int(x[0]) if x[0].isdigit() else x[0]   # 数字店舗は数値で昇順
        )
    )

    return data_sorted
