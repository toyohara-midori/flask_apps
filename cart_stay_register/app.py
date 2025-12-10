from flask import Blueprint, render_template, request, jsonify, redirect, url_for
from common.cucd_logic import get_cucd_list, check_cucd
from common.db_connection import get_connection
from datetime import date, datetime
from auth.auth_utils import login_required

# Blueprint登録
cart_bp = Blueprint(
    "cart_stay_register",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static"
)

# -------------------------
# 滞留カゴ車登録画面 表示
# -------------------------
@cart_bp.route("/", methods=["GET"])
@login_required
def cart_stay_index():
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

@cart_bp.route("/api/cart_category_list")
@login_required
def api_category_list():
    sql = "SELECT catcd, catname FROM dbo.CartCategory ORDER BY catcd"
    with get_connection("SQLS08-14") as conn:
        cur = conn.cursor()
        rows = cur.execute(sql).fetchall()

    result = {str(r.catcd): r.catname for r in rows}
    return jsonify(result)

# -------------------------
# 日付入力処理
# -------------------------
@cart_bp.route("/api/check_date", methods=["POST"])
@login_required
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
                SELECT catcd, count
                FROM CartStayCount
                WHERE cucd = ? AND idleDate = ?
            """
            cur.execute(sql, (cucd, idle_date))
            rows = cur.fetchall()

        # データが1件もない場合
        if not rows:
            return jsonify(
                ok=True, exists=False,
                cat1=0, cat2=0, cat3=0, cat4=0,
                msg="新規登録です"
            )

        # catcd → フィールドに振り分ける
        cat_values = {1: 0, 2: 0, 3: 0, 4: 0}
        for r in rows:
            cd = int(r.catcd)
            if cd in cat_values:
                cat_values[cd] = r.count

        return jsonify(
            ok=True,
            exists=True,
            cat1=cat_values[1],
            cat2=cat_values[2],
            cat3=cat_values[3],
            cat4=cat_values[4],
            msg="既に登録があります"
        )

    except Exception as e:
        return jsonify(ok=False, msg=f"DBアクセスエラー: {e}")

# -------------------------
# 登録ボタン処理
# -------------------------
@cart_bp.route("/api/register_cart", methods=["POST"])
@login_required
def api_register_cart():
    payload = request.get_json(force=True) or {}
    cucd = (payload.get("cucd") or "").strip()
    idle_date = (payload.get("idleDate") or "").strip()

    # HTMLから送られる値
    cat_values = {
        1: int(payload.get("cat1") or 0),
        2: int(payload.get("cat2") or 0),
        3: int(payload.get("cat3") or 0),
        4: int(payload.get("cat4") or 0),
    }

    # 入力チェック
    if not cucd or len(cucd) != 3 or not cucd.isdigit():
        return jsonify(ok=False, msg="正しい店舗CDを入力してください", focus="cucd")
    if not idle_date:
        return jsonify(ok=False, msg="日付を正しく入力してください", focus="date")

    try:
        with get_connection("SQLS08-14") as conn:
            cur = conn.cursor()

            # ① 既存データ取得
            cur.execute("""
                SELECT catcd, count 
                FROM CartStayCount
                WHERE cucd = ? AND idleDate = ?
            """, (cucd, idle_date))

            rows = cur.fetchall()  # catcd=1〜4 のデータ（存在しないものもあり）

            # dict に変換（存在しない catcd は0扱い）
            old_values = {1: 0, 2: 0, 3: 0, 4: 0}
            for r in rows:
                old_values[int(r.catcd)] = r.count

            # ② 完全一致チェック（既存と入力値が同じなら更新不要）
            if all(old_values[k] == cat_values[k] for k in old_values):
                return jsonify(ok=True, msg="既に同じ登録があります")

            # ③ catcd 1〜4 をループで insert/update
            for catcd in [1, 2, 3, 4]:
                new_count = cat_values[catcd]

                if old_values[catcd] == 0 and new_count != 0:
                    # INSERT
                    cur.execute("""
                        INSERT INTO CartStayCount (cucd, idleDate, catcd, count, rgtm)
                        VALUES (?, ?, ?, ?, GETDATE())
                    """, (cucd, idle_date, catcd, new_count))

                elif old_values[catcd] != 0:
                    # UPDATE（値が0でも更新する）
                    cur.execute("""
                        UPDATE CartStayCount
                        SET count = ?, rgtm = GETDATE()
                        WHERE cucd = ? AND idleDate = ? AND catcd = ?
                    """, (new_count, cucd, idle_date, catcd))

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
@login_required
def api_cucd_list():
    return jsonify(get_cucd_list())

@cart_bp.route("/api/chk_cucd", methods=["POST"])
@login_required
def api_chk_cucd():
    payload = request.get_json(force=True) or {}
    cucd = payload.get("cucd", "")
    return jsonify(check_cucd(cucd))
