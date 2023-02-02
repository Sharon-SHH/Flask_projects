import random
import sys
from faker import Faker
from bootstrap_table_sm import db, User
import time
import os
from sqlalchemy import create_engine, select, desc, asc
from sqlalchemy.orm import sessionmaker

basedir = os.path.abspath(os.path.dirname(__file__))
engine = create_engine('sqlite:///'+ os.path.join(basedir, 'small.sqlite')) #'sqlite:///db.sqlite'
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

def query_col_users():
    query = User.query
    col_name = 'age'
    col = getattr(User, col_name)
    # User.query.
    for user in query:
        print(f'name: {user.name}, age: {user.age}')

def query_users():
    query = User.query
    for user in query:
        print(f'name: {user.name}, age: {user.age}')

def filter_users():
    col_name = 'age'
    col = getattr(User, col_name)
    query = User.query.filter(col==29)
    for user in query:
        print(f'name: {user.name}, age: {user.age}')
    print(f'match: {query.count()}')

def sort_users(descending=False):
    order = []
    col_name = 'name'
    if col_name not in ['name', 'age', 'email']:
        col_name = 'name'

    col = getattr(User, col_name)
    if descending:
        col = col.desc()
    else:
        col = col.asc()
    order.append(col)
    query = User.query
    query = query.order_by(*order)
    for user in query:
         print(f'name: {user.name}, age: {user.age}')


def manipulate_cols():
    # age - > get year, add to email address
    order = []
    col_name = 'name'
    if col_name not in ['name', 'age', 'email']:
        col_name = 'name'

    col = getattr(User, col_name)
    query = User.query
    for user in query:
        print(f'name: {user.name}, age: {user.age}')

import pandas as pd
def create_pickle():
    df = pd.DataFrame()
    for col in User.__table__.columns.keys():
        name = getattr(User, col)
        df[col]  = session.query(name).all()

        # for item in query:
        #     d[col.name] = item.name
        # data[col.name] = [item[col.name] for item in query]
    df = df.drop(columns=['id'])
    df.to_pickle('User.pkl')
    unpickle_df = pd.read_pickle('User.pkl')
    print(unpickle_df)
    # for item in query:
    #     print(item.__dict__)
    # # get each column User.__table__.columns
    # df = pd.DataFrame.from_dict(data) # data: dictionary
    # df.to_pickle()
def read_pi_User():
    start = time.time()
    unpickle_df = pd.read_pickle('User.pkl')
    print(f'read_pi_User takes {time.time() - start} s.')

def sort_pi_User():
    start = time.time()
    unpickle_df = pd.read_pickle('User.pkl')
    unpickle_df = unpickle_df.sort_values(by=['name'],  ascending=True)
    # unpickle_df.to_pickle('User.pkl')
    # print(unpickle_df)
    print(f'read_pi_User takes {time.time() - start} s.')
def manipulate_pi_User():
    start = time.time()
    df = pd.read_pickle('User.pkl')
    # df['period'] = df['name'].astype(str)+'-'+df['first_name']
    df['full_name'] = df.name.astype(str).str.cat(df.first_name.astype(str), sep='q')
    # a = df['name'].to_frame()
    # b = df['first_name'].to_frame()
    print(f'manipulate_pi_User takes {time.time() - start} s.')


if __name__ == '__main__':
    # if len(sys.argv) <= 1:
    #     print('Pass the number of users you want to create as an argument.')
    #     sys.exit(1)
    # remove_users()
    # create_fake_users(100000)
    # count_users()
    # create_pickle()
    # ------------------------------------pickle file-----------------------
    # read_pi_User()
    sort_pi_User()
    # manipulate_pi_User()
    # ----------------------------
    # query_col_users()
    # start = time.time()
    # query_users()
    # print(time.time() - start)
    # filter_users()
    # print(time.time() - start)
    # sort_users(True)
    # print(time.time() - start)
