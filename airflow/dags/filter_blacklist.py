import pandas as pd

transaction_customer_file = 'transaction_customer.json'
transaction_customer_df = pd.read_json(transaction_customer_file, lines=True)
with open('blacklisted.txt') as fopen:
    blacklisted = fopen.readlines()
print(blacklisted)
blacklisted = [int(id.strip()) for id in blacklisted]
print(blacklisted)

clean_df = transaction_customer_df[~transaction_customer_df['customer_id'].isin(blacklisted)]
print(clean_df.shape)
output_file = 'filtered_transaction.json'
clean_df.to_json(output_file, orient='records', lines=True)
