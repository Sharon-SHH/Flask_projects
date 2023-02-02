from flask import Flask, render_template
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///small.sqlite'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(250), index=True)
    first_name = db.Column(db.String(64), index=True)
    last_name = db.Column(db.String(64), index=True)
    age = db.Column(db.Integer, index=True)
    address = db.Column(db.String(256))
    phone = db.Column(db.String(20))
    email = db.Column(db.String(120), index=True)
    city = db.Column(db.String(64))
    color = db.Column(db.String(20))
    ssn = db.Column(db.String(64))
    job = db.Column(db.String(64))
    currency_code = db.Column(db.String(250))
    def __repr__(self):
        return f"User(name={self.name}, first_name={self.first_name}, last_name={self.last_name}, age={self.age}," \
               f"address={self.address}, phone={self.phone}, email={self.email}, \n city={self.city}, color={self.color}, " \
               f"ssn={self.ssn}, job={self.job}, current_code={self.currency_code})"


db.create_all()


# @app.route('/')
# def index():
#     users = User.query
#     return render_template('bootstrap_table.html', title='Bootstrap Table',
#                            users=users)
#
#
# if __name__ == '__main__':
#     app.run()
