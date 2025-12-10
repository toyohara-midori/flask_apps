from flask import (
    Blueprint, render_template, request, redirect,
    url_for, session
)
from . import auth_bp
from .ip_utils import extract_store_from_ip
from .db_auth import authenticate_employee
import time

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    error = ""
    client_ip = request.remote_addr

    # 初期表示時：IPから店舗番号を算出
    store_cd = extract_store_from_ip(client_ip)
    if not store_cd:
        error = "店舗CDが取得できません"

    if request.method == "POST":
        # ★ POST のときだけ name を探す
        emp_no_key = next((k for k in request.form.keys() if k.startswith("emp_no_")), None)
        if not emp_no_key:
            return render_template("auth/login.html", store_cd=store_cd, error="社員番号の入力欄が取得できません")

        emp_no = request.form[emp_no_key].strip()
        store_cd_post = request.form.get("store_cd", "").strip()

        # 入力チェック
        if not emp_no.isdigit() or len(emp_no) != 7:
            error = "社員番号は7桁の数字で入力してください"
        else:
            # SQL認証
            if authenticate_employee(emp_no, store_cd_post):
                session["employee"] = {
                    "emp_no": emp_no,
                    "store_cd": store_cd_post,
                }

                session["last_access"] = time.time()    # 現在時刻を保持(無操作5分後ログアウト

                next_url = request.args.get("next") or url_for("cart_stay_register.cart_stay_index")
                return redirect(next_url)

            error = "認証不可の社員番号です"

    return render_template(
        "auth/login.html",
        store_cd=store_cd,
        error=error
    )

# ログアウト
@auth_bp.route("/logout", methods=["POST"])
def logout():
    session.clear()  # ← すべてのセッション情報を消す
    return redirect(url_for("auth.login"))


# ログインを継続させるかどうかのチェックAPI
@auth_bp.route("/check_session")
def check_session():
    import time
    from flask import jsonify, session

    now = time.time()
    last = session.get("last_access", now)

    # ★ タイムアウト判定（5分=300秒）
    if now - last > 300:
        session.clear()
        return jsonify({"logged_in": False})

    return jsonify({"logged_in": True})

