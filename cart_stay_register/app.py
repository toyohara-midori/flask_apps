from flask import Blueprint, render_template, request, jsonify, redirect, url_for
from common.cucd_logic import get_cucd_list, check_cucd
from common.db_connection import get_connection
from datetime import date, datetime

# Blueprint登録
cart_bp = Blueprint(
    "cart_stay_register",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/flask/cart_stay_register/static"   # IIS 配下のURLと合わせる
)

# -------------------------
# 滞留カゴ車登録画面 表示
# -------------------------
@cart_bp.route("/", methods=["GET"])
def index():
    today_str = date.today().strftime("%Y-%m-%d")
    return render_template(
        "cart_stay_register.html",
        cucd="",
        date=today_str,
        kubun1="",
        kubun2="",
        kubun3="",
        kubun4="",
        message=""
    )

# -------------------------
# 日付入力処理
# -------------------------
@cart_bp.route("/api/check_date", methods=["POST"])
def api_check_date():
    payload = request.get_json(force=True) or {}
    cucd = (payload.get("cucd") or "").strip()
    idle_date = (payload.get("idleDate") or "").strip()

    if not cucd or not idle_date:
        return jsonify(ok=False, msg="店舗CDと日付を入力してください")

    try:
        with get_connection("SQLS08-14") as conn:
            cur = conn.cursor()
            sql = """
                SELECT cat1, cat2, cat3, cat4
                FROM CartStayCount
                WHERE cucd = ? AND idleDate = ?
            """
            cur.execute(sql, (cucd, idle_date))
            row = cur.fetchone()

        if row:
            return jsonify(ok=True, exists=True,
                           cat1=row[0], cat2=row[1], cat3=row[2], cat4=row[3],
                           msg="既に登録があります")
        else:
            return jsonify(ok=True, exists=False,
                           cat1=0, cat2=0, cat3=0, cat4=0,
                           msg="新規登録です")
    except Exception as e:
        return jsonify(ok=False, msg=f"DBアクセスエラー: {e}")

# -------------------------
# 登録ボタン処理
# -------------------------
@cart_bp.route("/api/register_cart", methods=["POST"])
def api_register_cart():
    payload = request.get_json(force=True) or {}
    cucd = (payload.get("cucd") or "").strip()
    idle_date = (payload.get("idleDate") or "").strip()
    cat1 = int(payload.get("cat1") or 0)
    cat2 = int(payload.get("cat2") or 0)
    cat3 = int(payload.get("cat3") or 0)
    cat4 = int(payload.get("cat4") or 0)

    if not cucd or len(cucd) != 3 or not cucd.isdigit():
        return jsonify(ok=False, msg="正しい店舗CDを入力してください", focus="cucd")
    if not idle_date:
        return jsonify(ok=False, msg="日付を正しく入力してください", focus="date")

    try:
        with get_connection("SQLS08-14") as conn:
            cur = conn.cursor()

            cur.execute("""
                SELECT cat1, cat2, cat3, cat4
                FROM CartStayCount
                WHERE cucd = ? AND idleDate = ?
            """, (cucd, idle_date))
            row = cur.fetchone()

            if row:
                if (row[0], row[1], row[2], row[3]) == (cat1, cat2, cat3, cat4):
                    return jsonify(ok=True, msg="既に同じ登録があります")

                cur.execute("""
                    UPDATE CartStayCount
                    SET cat1=?, cat2=?, cat3=?, cat4=?, rgtm=?
                    WHERE cucd=? AND idleDate=?
                """, (cat1, cat2, cat3, cat4, datetime.now(), cucd, idle_date))
                conn.commit()
                return jsonify(ok=True, msg="更新しました")

            cur.execute("""
                INSERT INTO CartStayCount (cucd, idleDate, cat1, cat2, cat3, cat4, rgtm)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (cucd, idle_date, cat1, cat2, cat3, cat4, datetime.now()))
            conn.commit()

        return jsonify(ok=True, msg="登録しました")

    except Exception as e:
        return jsonify(ok=False, msg=f"DBエラー: {e}")

# -------------------------
# 終了（トップメニューへ戻る）
# -------------------------
@cart_bp.route("/exit")
def exit_page():
    return redirect(url_for("home"))

# -------------------------
# common のAPI
# -------------------------
@cart_bp.route("/api/cucd_list")
def api_cucd_list():
    return jsonify(get_cucd_list())

@cart_bp.route("/api/chk_cucd", methods=["POST"])
def api_chk_cucd():
    payload = request.get_json(force=True) or {}
    cucd = payload.get("cucd", "")
    return jsonify(check_cucd(cucd))
