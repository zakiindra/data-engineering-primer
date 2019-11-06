import requests
import pandas as pd

response = requests.get('https://us-central1-bi-dwhdev-02.cloudfunctions.net/customer')
data = response.json()
customer_df = pd.DataFrame(data)
output_file = 'customer.json'
customer_df.to_json(output_file, orient='records', lines=True)