import pandas as pd

filtered_file = 'filtered_transaction.json'
transaction_df = pd.read_json(filtered_file, lines=True)

success_df = transaction_df[transaction_df['status'].isin(['SUCCESS'])]

grouped_df = success_df.groupby(['city'])[['city','amount_rupiah']].sum()

java_cities = ['Jakarta', 'Bandung', 'Surabaya', 'Yogyakarta']
final_df = grouped_df[grouped_df['city'].isin(java_cities)]

output_file = 'java_total_transactions.json'
final_df.to_json(output_file, orient='records', lines=True)
