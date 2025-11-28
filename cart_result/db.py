from datetime import timedelta, date
from common.db_connection import get_connection

def get_week_calendar(year: int):
    """
    年度カレンダーを返す。
    ・第1週はその年の 2〜3月の最初の月曜日
    ・年度末は翌年の第1週の月曜日の前日
    """

    with get_connection("SQLS08-14") as conn:
        cur = conn.cursor()

        # --------------------------
        # ① その年の第1週の開始日（月曜日）
        # --------------------------
        cur.execute("""
            SELECT date_s
            FROM DBA.weekno2
            WHERE date_s >= ? AND date_s < ? AND weekno = 1
            ORDER BY date_s
        """, (f"{year}-02-01", f"{year}-04-01"))
        rows = cur.fetchall()

        start_date = None
        for (ds,) in rows:
            ds = ds.date() if hasattr(ds, "date") else ds
            if ds.weekday() == 0:  # 0=月曜日
                start_date = ds
                break

        if start_date is None:
            raise ValueError(f"{year}年度の開始(月曜日)が見つかりません")

        # --------------------------
        # ② 翌年の第1週の開始日（月曜日）
        # --------------------------
        next_year = year + 1
        cur.execute("""
            SELECT date_s
            FROM DBA.weekno2
            WHERE date_s >= ? AND date_s < ? AND weekno = 1
            ORDER BY date_s
        """, (f"{next_year}-02-01", f"{next_year}-04-01"))
        rows = cur.fetchall()

        next_start = None
        for (ds,) in rows:
            ds = ds.date() if hasattr(ds, "date") else ds
            if ds.weekday() == 0:
                next_start = ds
                break

        if next_start is None:
            raise ValueError(f"{next_year}年度の開始(月曜日)が見つかりません")

        # --------------------------
        # ③ 年度末日 = 翌年第1週の前日（日曜日）
        # --------------------------
        end_date = next_start - timedelta(days=1)

        # --------------------------
        # ④ 対象期間にかかる weekno2 を取得
        # --------------------------
        cur.execute("""
            SELECT weekno, date_s, date_e
            FROM DBA.weekno2
            WHERE date_e >= ? AND date_s <= ?
            ORDER BY date_s
        """, (start_date, end_date))
        weeks = cur.fetchall()

    # --------------------------
    # ⑤ 各週の実日付に分解
    # --------------------------
    days = []
    for weekno, ds, de in weeks:
        s = ds.date() if hasattr(ds, "date") else ds
        e = de.date() if hasattr(de, "date") else de

        cur_date = s
        while cur_date <= e:
            if start_date <= cur_date <= end_date:
                days.append((weekno, cur_date))
            cur_date += timedelta(days=1)

    return days

def fetch_cart_stay_all(year: int):
    """
    指定年度の cat1〜cat4 のデータをすべて取得し、
    {"CUCD_YYYY-MM-DD": {"cat1": x, "cat2": y, ...}} の辞書形式で返す。
    """

    # ① まず年度カレンダーから「開始日・終了日」を決める
    days = get_week_calendar(year)  # [(weekno, date_obj), ...]
    if not days:
        return {}

    start_date = days[0][1]   # 一番最初の日付
    end_date   = days[-1][1]  # 一番最後の日付

    with get_connection("SQLS08-14") as conn:
        cur = conn.cursor()

        # ② idleDate を年度の日付範囲で絞る（←ここがポイント）
        sql = """
            SELECT cucd, idleDate, cat1, cat2, cat3, cat4
            FROM CartStayCount
            WHERE idleDate BETWEEN ? AND ?
        """
        cur.execute(sql, (start_date, end_date))
        rows = cur.fetchall()

    data_dict = {}

    for row in rows:
        cucd = str(row.cucd).strip().upper()

        idle_date = row.idleDate
        if hasattr(idle_date, "date"):
            idle_date = idle_date.date()

        ymd = idle_date.strftime("%Y-%m-%d")
        key = f"{cucd}_{ymd}"

        data_dict[key] = {
            "cat1": row.cat1,
            "cat2": row.cat2,
            "cat3": row.cat3,
            "cat4": row.cat4,
        }

    return data_dict


def fetch_total_for_date_and_kbn(target_date: date, kbn_no):
    """
    指定日・区分の合計値を CartStayCount から取得する。
      kbn_no: 1〜4 → cat1〜cat4
              "total" → cat1〜cat4 の合計
    第1週の、前週差分を計算する時に使う。
    """
    with get_connection("SQLS08-14") as conn:
        cur = conn.cursor()

        if kbn_no == "total":
            sql = """
                SELECT
                    COALESCE(SUM(cat1),0)
                  + COALESCE(SUM(cat2),0)
                  + COALESCE(SUM(cat3),0)
                  + COALESCE(SUM(cat4),0)
                FROM CartStayCount
                WHERE idleDate = ?
            """
            cur.execute(sql, (target_date,))
        else:
            # kbn_no は 1〜4 を想定
            col = f"cat{kbn_no}"
            sql = f"""
                SELECT COALESCE(SUM({col}),0)
                FROM CartStayCount
                WHERE idleDate = ?
            """
            cur.execute(sql, (target_date,))

        row = cur.fetchone()
        return row[0] or 0

