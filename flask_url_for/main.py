from flask import Flask, render_template

app = Flask(__name__)
#dir = os.path.dirname(os.getcwd()) # get the parent directory of the current working directory


@app.route("/")
def home():  # the function of the name will be used for the function url_for.
    return render_template("home.html")

@app.route("/login")
def login():
    return render_template("login.html")

@app.route("/signup")
def signup():
    return render_template("signup.html")

if __name__ == '__main__':
    app.run(debug=True)