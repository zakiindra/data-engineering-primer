import pandas as pd

customer_file = 'customer.json'
transaction_file = 'transaction.json'
customer_df = pd.read_json(customer_file, lines=True)
transaction_df = pd.read_json(transaction_file, lines=True)
joined_df = pd.merge(transaction_df, customer_df, on='customer_id')
output_file = 'transaction_customer.json'
joined_df.to_json(output_file, orient='records', lines=True)
