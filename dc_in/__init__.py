from flask import Blueprint, request, abort, render_template_string
# ★既存の共通部品をインポート
from common.auth_util import get_remote_user
from common.ad_tool import is_user_in_group
from common.db_connection import get_connection

dc_in_bp = Blueprint('dc_in', __name__, url_prefix='/dc_in', template_folder='templates')

# メンテナンス表示用HTML
MAINTENANCE_HTML = "<h1>現在メンテナンス中、またはDB接続エラーです。</h1>"

def is_db_available():
    try:
        conn = get_connection('master')
        conn.close()
        return True
    except:
        return False

@dc_in_bp.before_request
def before_request_handler():
    # 静的ファイルへのアクセスはチェックしない
    if not request.endpoint or 'static' in request.endpoint:
        return

    # 1. DB接続チェック
    if not is_db_available():
        return render_template_string(MAINTENANCE_HTML), 503

    # 2. ユーザー特定 (common.auth_util を使用)
    user = get_remote_user(request)
    if not user:
        # ユーザーが取れない＝認証されていない
        abort(401)
    
    # 後の処理で使えるようにリクエスト毎の変数に入れておくのも良いですが、
    # ここではログ出力のみ行います
    # print(f"Access User: {user}")

    # 3. ADグループ認証 (common.ad_tool を使用)
    allowed_groups = [
        'Domain Admins',
        'G-商品部ディストリビューター',
        'G-商品部バイヤー'
    ]
    
    has_permission = False
    
    # ★開発環境などでADにつながらない場合の緊急回避（必要ならコメントアウトを外す）
    # if user == 'Unknown' or os.environ.get('FLASK_ENV') == 'development':
    #     has_permission = True
    # else:
    
    try:
        for group in allowed_groups:
            if is_user_in_group(user, group):
                has_permission = True
                break
    except Exception as e:
        print(f"[AD Auth Error] {e}")
        # 認証サーバーエラー時は安全のため拒否
        abort(403, description="認証サーバーへの接続に失敗しました。")

    if not has_permission:
        print(f"[Access Denied] User: {user}")
        abort(403, description="この機能を利用する権限がありません。")

from . import views