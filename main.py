import os
from xiaoke_gateway_api import create_app

app = create_app(os.environ.get('XIAOKE_DB_PATH', 'data/xiaoke.sqlite'))

