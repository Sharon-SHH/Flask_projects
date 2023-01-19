import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

filepath = os.path.realpath(__file__)
parent = os.path.dirname(filepath)
sq_path = 'sqlite:///'+ os.path.join(parent, 'data/marret.sqlite')
app.config['SQLALCHEMY_DATABASE_URI'] = sq_path
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
#dir = os.path.dirname(os.getcwd()) # get the parent directory of the current working directory
db = SQLAlchemy(app)
db.drop_all() # remove all tables

class Returns(db.Model):
    # id = db.Column(db.Integer, primary_key=True)
    __tablename__ = 'returns'
    __table_args__ = {'extend_existing': True}
    cusip_code = db.Column(db.String(256), primary_key=True)
    asset_class = db.Column(db.String(20))
    class_ = db.Column(db.String(20))
    security_name = db.Column(db.String(120))
    yield_worst = db.Column(db.String(50))      # YTW
    price_final_yest = db.Column(db.String(50))  # Px Yest, String
    price_final = db.Column(db.Float)       # Px Today
    dollar_chg = db.Column(db.Float)        # Px Chg$
    price_chg = db.Column(db.Float)         # Px Chg%
    # Spread
    spread_final_yest = db.Column(db.Float) # Spr Yest
    spread_final = db.Column(db.Float)      # Spr Today
    spread_chg = db.Column(db.Float)        # Spr Chg
    benchname_final = db.Column(db.Float)   # Benchmark

    # Override
    source_ov = db.Column(db.String(20))  # TODO: dropdown
    price_ov_yest = db.Column(db.Float)   # Yes Px Ov
    price_ov = db.Column(db.Float)        # Px OV
    spread_ov = db.Column(db.Float)       # Spr OV
    bench_ov = db.Column(db.Float)        # Benchmark Ov

    # hidden columns
    new_i = db.Column(db.String(50))

    def to_dict(self):
        return {
            'Asset': self.asset_class,
            'Group': self.class_,
            'Cusip': self.cusip_code,
            'Security Name': self.security_name,
            'YTW': self.yield_worst,
            'Px Yest': self.price_final_yest,
            'Px Today': self.price_final,
            'Px Chg$': self.dollar_chg,
            'Px Chg%': self.price_chg,
            # Spread
            'Spr Yest': self.spread_final_yest,
            'Spr Today': self.spread_final,
            'Spr Chg': self.spread_chg,
            'Benchmark': self.benchname_final,
            # Override
            'Source': self.source_ov,
            'Yes Px Ov': self.price_ov_yest,
            'Px Ov': self.price_ov,
            'Spr Ov': self.spread_ov,
            'Benchmark Ov': self.bench_ov,
            'New Issue': self.new_i # TODO: remove to be hidden
        }

class Account(db.Model):
    __table_args__ = {'extend_existing': True}
    account_id = db.Column(db.String(20), primary_key=True)
    account_style = db.Column(db.String(50))
    groups = db.Column(db.String(100))

    def __init__(self, account_id, account_style, groups):
        self.account_id = account_id
        self.account_style = account_style
        self.groups = groups

    def __repr__(self):
        return f'{self.account_id}'  # f'{self.account_id} has {self.account_style} in {self.groups}'

    def to_dict(self):
        return {
            'account_id': self.account_id,
            'account_style': self.account_style,
            'groups': self.groups
        }


db.create_all() # create all the databases. Table models -> db table