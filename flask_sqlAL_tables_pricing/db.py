import sqlalchemy as db

server_name = "HDSQL01\INS2"
dbname = "MARRET"
engine = db.create_engine('mssql+pyodbc://'+ server_name + '/' + dbname +
                                  '?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server')
cnxn = engine.connect()

metadata = db.MetaData() #extracting the metadata
division= db.Table('PORTFOLIO_MASTER_PRICING', metadata, autoload=True, autoload_with=engine)

query = division.select()
exe = cnxn.execute(query)
#exe = cnxn.execute("SELECT * FROM PORTFOLIO_MASTER_PRICING")

res = exe.fetchmany(10)
print(res)