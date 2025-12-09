from flask import Blueprint

# Blueprintの作成
# template_folder='templates' とすることで、このフォルダ内の templates を参照できるようにする
dc_in_bp = Blueprint('dc_in', __name__, template_folder='templates')

# viewsを読み込んでルートを登録させる
from . import views
