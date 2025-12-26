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

    # ① 年度カレンダー取得（開始日・終了日）
    days = get_week_calendar(year)
    if not days:
        return {}

    start_date = days[0][1]
    end_date   = days[-1][1]

    with get_connection("SQLS08-14") as conn:
        cur = conn.cursor()

        # ② 新テーブル構造に対応した SELECT（catcd / count）
        sql = """
            SELECT cucd, idleDate, catcd, count
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

        # dict が未作成なら初期化
        if key not in data_dict:
            data_dict[key] = {"cat1": 0, "cat2": 0, "cat3": 0, "cat4": 0}

        # catcd → cat1〜cat4 にセット
        catcd = int(row.catcd)
        if 1 <= catcd <= 4:
            data_dict[key][f"cat{catcd}"] = row.count

    return data_dict


def fetch_total_for_date_and_kbn(target_date: date, kbn_no):
    """
    指定日・区分の合計値を CartStayCount から取得する。
      kbn_no: 1〜4 → catcdごとの合計
              "total" → catcd 1〜4 の合計
    第1週の、前週差分を計算する時に使う。
    """
    with get_connection("SQLS08-14") as conn:
        cur = conn.cursor()

        if kbn_no == "total":
            # catcd 1〜4 の全部
            sql = """
                SELECT COALESCE(SUM(count), 0)
                FROM CartStayCount
                WHERE idleDate = ?
                  AND catcd IN (1, 2, 3, 4)
            """
            cur.execute(sql, (target_date,))
        else:
            # kbn_no は 1〜4
            sql = """
                SELECT COALESCE(SUM(count), 0)
                FROM CartStayCount
                WHERE idleDate = ?
                  AND catcd = ?
            """
            cur.execute(sql, (target_date, kbn_no))

        row = cur.fetchone()
        return row[0] or 0

# ============================================================
# 2週間データ専用の fetch 関数
# ============================================================
def fetch_cart_stay_period(start_date, end_date):
    with get_connection("SQLS08-14") as conn:
        cur = conn.cursor()
        sql = """
            SELECT cucd, idleDate, catcd, count
            FROM CartStayCount
            WHERE idleDate BETWEEN ? AND ?
        """
        cur.execute(sql, (start_date, end_date))
        rows = cur.fetchall()

    data_dict = {}
    for r in rows:
        cucd = str(r.cucd).strip()
        d = r.idleDate.date() if hasattr(r.idleDate, "date") else r.idleDate
        key = f"{cucd}_{d:%Y-%m-%d}"

        if key not in data_dict:
            data_dict[key] = {"cat1": 0, "cat2": 0, "cat3": 0, "cat4": 0}

        data_dict[key][f"cat{r.catcd}"] = r.count

    return data_dict

# ============================================================
# 区分の内容を取得する関数
# ============================================================
def get_category_titles():
    """
    CartCategory テーブルから catcd, catname を読み取り、
    {1: "区分1：○○", 2: "区分2：○○", ...} の形式で返す
    """
    with get_connection("SQLS08-14") as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT catcd, catname
            FROM CartCategory
            ORDER BY catcd
        """)
        rows = cur.fetchall()

    title_map = {}

    for r in rows:
        cd = int(r.catcd)
        name = str(r.catname).strip()
        title_map[cd] = f"区分{cd}：{name}"

    return title_map

def get_cucd_info_map():
    """
    CucdInfo + custype + cq12cucd から
    floorSpace, scmCucd(type), vehicle(判定後文字列) を取得し、
    cucd をキーとする辞書で返す。
    """

    # -------------------------
    # ① SQLS08-14 側データ取得
    # -------------------------
    with get_connection("SQLS08-14") as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                ci.cucd,
                ci.floorSpace,
                ct.type AS scmCucd
            FROM dbo.CucdInfo ci
            LEFT JOIN DBA.custype ct
                ON ct.cucd = ci.cucd
        """)
        rows = cur.fetchall()

    # -------------------------
    # ② master 側 yoseki 取得
    # -------------------------
    with get_connection("master") as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                cucd,
                yoseki
            FROM cq12cucd
        """)
        yoseki_rows = cur.fetchall()

    # cucd → yoseki の辞書
    yoseki_map = {
        str(r.cucd).strip(): r.yoseki
        for r in yoseki_rows
    }

    # -------------------------
    # ③ データ組み立て
    # -------------------------
    info = {}

    for r in rows:
        cucd = str(r.cucd).strip()

        scm = "" if r.scmCucd is None else str(r.scmCucd).strip()

        yoseki = yoseki_map.get(cucd)

        # vehicle 判定
        if yoseki == 20085000:
            vehicle = "10トン"
        else:
            vehicle = "増トン"

        info[cucd] = {
            "floorSpace": r.floorSpace,
            "scmCucd": scm,     # '003' / '004'
            "vehicle": vehicle # ← 新仕様
        }

    return info

