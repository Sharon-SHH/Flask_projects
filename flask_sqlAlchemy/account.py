from Flask_projects_learning.flask_sqlAlchemy.app import db
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
        return f'{self.account_id}' #f'{self.account_id} has {self.account_style} in {self.groups}'

    def to_dict(self):
        return {
            'account_id': self.account_id,
            'account_style': self.account_style,
            'groups': self.groups
        }