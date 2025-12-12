from flask import Blueprint, request, abort, render_template_string
from common.db_check_util import is_db_available, MAINTENANCE_HTML
from common.auth_util import get_remote_user
from common.ad_tool import check_permission_via_command, create_access_denied_html

# Blueprint定義
hacfl_bp = Blueprint(
    'hacfl',
    __name__,
    template_folder='templates',
    url_prefix='/hacfl'  # URLプレフィックスを明確化
)

@hacfl_bp.before_request
def before_request_handler():
    # 1. 静的ファイルはスルー
    if request.endpoint and 'static' in request.endpoint:
        return

    # 2. DB接続チェック
    if not is_db_available():
        return render_template_string(MAINTENANCE_HTML), 503

    # 3. ユーザー特定 (IIS認証等)
    user = get_remote_user(request)
    if not user:
        abort(401)

    # 4. ADグループ権限チェック (net userコマンド版)
    # ★この機能を利用できるグループを定義
    allowed_groups = [
        'Domain Admins',
        'G-商品部ディストリビューター',
        'G-商品部バイヤー',
        'G-システム管理部'
    ]
    
    # 権限チェック実行 (common.ad_tool)
    if check_permission_via_command(user, allowed_groups):
        # OK: 処理続行
        pass
    else:
        # NG: エラー画面を返却して中断
        display_name = user.split('\\')[1] if '\\' in user else user
        return create_access_denied_html(display_name)

# Viewのインポート（循環参照回避のため末尾）
from . import views