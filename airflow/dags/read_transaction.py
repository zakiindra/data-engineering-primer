import pandas as pd
import sqlalchemy as sql

address = 'mysql+pymysql://root:tseldsaroot@35.198.239.34/tsel'
sql_engine = sql.create_engine(address)
query = "select * from transaction"
transaction_df = pd.read_sql_query(query, con=sql_engine)
transaction_df = transaction_df.rename(columns={'amount' : 'amount_rupiah'})
print(transaction_df)
output_file = 'transaction.json'
transaction_df.to_json(output_file, orient='records', lines=True)