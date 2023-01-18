from flask import Flask, render_template


app = Flask(__name__)

todos = [("get milk", False), ("Learn Programming", True)]
@app.route("/")
def todo():
	return render_template('home.html', todos=todos)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8000, debug=True)