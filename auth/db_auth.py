# =================
# SQLで認証する関数
# =================

from common.db_connection import get_connection

def authenticate_employee(emp_no: str, store_cd: str) -> bool:
    """
    SQLS08-14 の empmst で照合する
    empcd (varchar7), cucd (varchar3)
    """
    sql = """
        SELECT COUNT(*)
        FROM DBA.empmst
        WHERE empcd = ? AND cucd = ?
    """

    with get_connection("SQLS08-14") as conn:
        cur = conn.cursor()
        cur.execute(sql, (emp_no, store_cd))
        cnt = cur.fetchone()[0]

    return cnt > 0
