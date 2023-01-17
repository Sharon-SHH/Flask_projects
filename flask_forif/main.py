from flask import Flask, render_template
import os

app = Flask(__name__)
filepath = os.path.realpath(__file__)
parent = os.path.dirname(filepath)
sq_path = 'sqlite:///'+ os.path.join(parent, 'data/Pricing.sqlite')
app.config['SQLALCHEMY_DATABASE_URI'] = sq_path
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
#dir = os.path.dirname(os.getcwd()) # get the parent directory of the current working directory


@app.route("/")
def todo():
    return render_template("set_parameter.html", title="Marret", todos=["get up", "work"], len=len)

if __name__ == '__main__':
    app.run(debug=True)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
