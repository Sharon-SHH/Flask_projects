import random
import os
from faker import Faker
from bootstrap_table import db, User
import time
from sqlalchemy import create_engine, select, desc, asc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import expression, functions
from sqlalchemy import types
import pandas as pd

basedir = os.path.abspath(os.path.dirname(__file__))
engine = create_engine('sqlite:///'+ os.path.join(basedir, 'db.sqlite')) #'sqlite:///db.sqlite'
Session = sessionmaker(bind=engine)
session = Session()

def create_fake_users(n):
    """Generate fake users."""
    faker = Faker()
    for i in range(n):
        user = User(name=faker.name(),
                    first_name=faker.first_name(),
                    last_name=faker.last_name(),
                    age=random.randint(20, 80),
                    address=faker.address().replace('\n', ', '),
                    phone=faker.phone_number(),
                    email=faker.email(),
                    city=faker.city(),
                    color=faker.color(),
                    ssn=faker.ssn(),
                    job=faker.job(),
                    currency_code=faker.currency_code()
                    )
        db.session.add(user)
    db.session.commit()
    print(f'Added {n} fake users to the database.')

def remove_users():
    db.drop_all()
    db.create_all()
    db.session.commit()

def count_users():
    query = User.query
    print(query.count())

def query_1col_users():
    start = time.time()
    session.query(User.name).all()
    print(f'query_1col takes {time.time() - start} s.')

def query_1row_users():
    start = time.time()
    session.query(User).get(1)
    print(f'query_1row takes {time.time() - start} s.')

def query_users():
    start = time.time()
    query = User.query
    print(f'query_users takes {time.time() - start} s.')

def filter_users():
    col_name = 'age'
    col = getattr(User, col_name)
    query = User.query.filter(col==29)
    for user in query:
        print(f'name: {user.name}, age: {user.age}')
    print(f'match: {query.count()}')

def sort_users(descending=False):
    start = time.time()
    if descending:
        session.query(User.name).order_by(User.name.desc()).all()
    else:
        session.query(User.name).order_by(User.name.asc()).all()
    print(f'Sort_users descending {descending==True} order, time {time.time()-start}')

def manipulate_cols():
    start = time.time()
    session.query((User.name+User.first_name+User.last_name).label('fullName')).all()
    print(f'manipulate_cols time: {time.time() - start}.')

if __name__ == '__main__':
    #remove_users()
    query_1col_users()
# query_1row_users()
#     create_fake_users(90000)
#     count_users()
#
    # query_users()
    # sort_users(False)
    # manipulate_cols()
# filter_users()