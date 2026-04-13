import boto3
import pandas as pd 

# Create S3 Client
s3 = boto3.client('s3')

bucket = 'utkarsh-de-project'
key = 'Raw/online_retail.csv'

# Read file from S3
obj = s3.get_object(Bucket=bucket,Key=key)

df = pd.read_csv(obj['Body'], encoding='ISO-8859-1')

print(df.head())
print('Shape',df.shape)

# Handle missing values
df['Description'] = df['Description'].fillna('Unknown')
df=df.dropna(subset=['CustomerID'])

# Fix data types
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
df['CustomerID'] = df['CustomerID'].astype(str)

# Remove Invalid Data
df = df[(df['Quantity']>0)&(df['UnitPrice']>0)]

# Create Revenue Column
df['Revenue'] = df['Quantity']*df['UnitPrice']

print('After Cleaning:',df.shape)

# Aggregation

summary = df.groupby('Country')['Revenue'].agg(['count','sum','mean'])
summary = summary.sort_values(by='sum',ascending=False).reset_index()

print(summary.head())

# Save Loacally
summary.to_parquet('summary.parquet', index=False)

# Upload to S3
s3.upload_file('summary.parquet',bucket,'Processed/summary.parquet')

print('Uploaded Processed data to S3')





