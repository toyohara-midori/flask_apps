from flask import Blueprint, request, abort, render_template_string
# 共通部品から「ユーザー取得」「権限チェック」「エラー画面生成」をインポート
from common.auth_util import get_remote_user
from common.ad_tool import check_permission_via_command, create_access_denied_html

dc_in_bp = Blueprint('dc_in', __name__, url_prefix='/dc_in', template_folder='templates')

@dc_in_bp.before_request
def before_request_handler():
    # 静的ファイルはチェックしない
    if not request.endpoint or 'static' in request.endpoint:
        return

    # 1. ユーザー特定
    user = get_remote_user(request)
    if not user:
        abort(401)

    # 2. 許可するグループ定義
    allowed_groups = [
        'Domain Admins',
        'G-商品部ディストリビューター',
        'G-商品部バイヤー'
    ]

    # 3. 権限チェック実行
    if check_permission_via_command(user, allowed_groups):
        # OKなら何もしない（正常処理へ進む）
        pass
    else:
        # NGなら、共通部品で作ったエラー画面を返す
        # 表示用にユーザー名からドメインを削る
        display_name = user.split('\\')[1] if '\\' in user else user
        return create_access_denied_html(display_name)

from . import views