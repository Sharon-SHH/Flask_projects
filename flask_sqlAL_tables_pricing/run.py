from db import cnxn
from flask import Flask

app = Flask(__name__)




if __name__ == '__main__':
    app.run(port=8999, debug=True)