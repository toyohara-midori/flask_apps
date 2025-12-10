# ==========================================
# ログイン必須デコレータ（特定ページを保護）
# ==========================================

import time
from functools import wraps
from flask import session, redirect, url_for, request

TIMEOUT_SECONDS = 300  # ★ 5分（300秒）

def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        # 未ログインならログイン画面へ
        if "employee" not in session:
            return redirect(url_for("auth.login", next=request.url))

        now = time.time()
        last = session.get("last_access", now)

        # ★ 5分以上操作なし → タイムアウト処理
        if now - last > TIMEOUT_SECONDS:
            session.clear()
            return redirect(url_for("auth.login"))

        # ★ アクセスがあるので最終アクセス時刻を更新
        session["last_access"] = now

        return fn(*args, **kwargs)

    return wrapper
