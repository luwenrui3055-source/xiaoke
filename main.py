import os
from xiaoke_gateway_api import create_app

app = create_app(os.environ.get('XIAOKE_DB_PATH', 'data/xiaoke.sqlite'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', '8080')), debug=False)

