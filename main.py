import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns
from prefect import task, flow
import dask.dataframe as dd
from sqlalchemy import create_engine

df = pd.read_csv('C:/Users/hp898/Downloads/Global_Superstore2.csv', encoding='ISO-8859-1')
print(df.head())
print(df.tail())
print(df.info())
print(df.describe())
print(df.isnull().sum())
print(df.duplicated())
print(df['Postal Code'])

df['Postal Code'].fillna(df['Postal Code'].mode()[0],inplace=True)
df['Profit Margin'] = df['Profit'] / df['Sales']
df['Order Year'] = pd.to_datetime(df['Order Date']).dt.year
df['Order Month'] = pd.to_datetime(df['Order Date']).dt.month
numeric_df = df.select_dtypes(include=['float64', 'int64'])
correlation_matrix = numeric_df.corr()
print(correlation_matrix)


plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt='.2f')
plt.title('Correlation Matrix Heatmap')
plt.show()


df.groupby('Category')['Sales'].sum().plot(kind='bar')
plt.title('Total Sales by Category')
plt.show()


df.groupby('Region')['Sales'].sum().plot(kind='pie', autopct='%1.1f%%')
plt.title('Sales Contribution by Region')
plt.show()


df.groupby('Order Year')['Sales'].sum().plot(kind='line')
plt.title('Yearly Sales Trend')
plt.show()


df['Total Sales'] = df['Quantity'] * df['Sales']
print(df['Total Sales'])
df['Order Date'] = pd.to_datetime(df['Order Date'])
df_filtered = df[['Order ID', 'Customer Name', 'Product Name', 'Quantity', 'Sales', 'Total Sales', 'Order Date',
'Region', 'Segment']]
df_filtered.to_csv('transformed_globalsuperstore.csv', index=False)


@task
def extract_data():
    df = dd.read_csv('C:/Users/hp898/Downloads/Global_Superstore2.csv', encoding='ISO-8859-1', assume_missing=True)
    print(f"Extracted data with {len(df)} rows.")
    return df

@task
def transform_data(df):
    df['Total Sales'] = df['Quantity'] * df['Sales']
    df['Order Date'] = dd.to_datetime(df['Order Date'], errors='coerce')
    df_filtered = df[['Order ID', 'Customer Name', 'Product Name', 'Quantity', 'Sales', 'Total Sales', 'Order Date',
'Region', 'Segment']]
    return df_filtered

@task
def load_data(df_filtered):
    output_path = 'transformed_global_superstore.csv'
    df_filtered.compute().to_csv(output_path, index=False)
    print(f"Data successfully loaded to {output_path}")

@flow
def etl_flow():
    df = extract_data()
    transformed_df = transform_data(df)
    load_data(transformed_df)
    return transformed_df

if __name__ == "__main__":
    etl_flow()

if __name__ == "__main__":
    transformed_df = etl_flow()

    conn_string = "mysql+pymysql://root:Rh8765844413@localhost:3306/mydatabase"
    engine = create_engine(conn_string)

    transformed_df.compute().to_sql('Orders', engine, if_exists='replace', index=False)

