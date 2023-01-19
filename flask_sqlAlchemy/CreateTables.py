import pandas as pd
from app import Account, db
from abc import ABC, abstractmethod


class CreateTable(ABC):
    @abstractmethod
    def create(self):
        pass

def accountDF():
    data = [['181', 'GV', 'GV, C1'], ['183', 'GV', 'GV, C1'], ['190', 'FI', 'IG, HY, C1'],
            ['125', 'IG', 'IG'], ['175', 'IG', 'IG'], ['184', 'IG', 'IG'], ['131', 'IGHF', 'IGHF'],
            ['218', 'HF', 'HF, C1'], ['174', 'SD', 'SD'], ['179', 'SD', 'SD'], ['192', 'SD', 'SD'],
            ['106', 'HY', 'HY'], ['162', 'HY', 'HY']]
    df = pd.DataFrame(data, columns=['ACCOUNT_ID', 'ACCOUNT_STYLE', 'GROUPs'])
    return df

class CreateAccount(CreateTable):
    def __init__(self):
        self.data = accountDF() # create dataframe of account
    def create(self):
        for item in self.data.itertuples():
            account = Account(account_id=item.ACCOUNT_ID, account_style=item.ACCOUNT_STYLE, groups=item.GROUPs)
            db.session.add(account)
        db.session.commit()


if __name__ == '__main__':
    # create tables for another use
    account_opt = CreateAccount()
    account_opt.create()


