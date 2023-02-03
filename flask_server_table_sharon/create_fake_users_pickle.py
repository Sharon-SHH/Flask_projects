import random
import time
import os
import pandas as pd
from sqlalchemy import create_engine
from faker import Faker
from bootstrap_table_sm import db, User
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


def create_pickle():
    df = pd.DataFrame()
    for col in User.__table__.columns.keys():
        name = getattr(User, col)
        df[col]  = session.query(name).all()
    df = df.drop(columns=['id'])
    df.to_pickle('User.pkl')
    unpickle_df = pd.read_pickle('User.pkl')
    print(unpickle_df)

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
    print(f'manipulate_pi_User takes {time.time() - start} s.')


if __name__ == '__main__':
    # remove_users()
    # create_fake_users(100000)
    # count_users()
    # create_pickle()
    # -----------pickle file------
    # read_pi_User()
    sort_pi_User()
    # manipulate_pi_User()
    # ----------------------------

