import pytest
from lib.utils import getSparkSession
from lib.dataReader import create_df,get_customers_schema
from lib.dataTransformation import filterDf
def test_df_count():
    spark = getSparkSession()
    customers_count = create_df(get_customers_schema(),'data/raw/olist_customers_dataset.csv').count()
    assert customers_count == 99441                           
    
def test_filter():
    customers_df = create_df(get_customers_schema(),'data/raw/olist_customers_dataset.csv')
    filterd_df = filterDf(customers_df,'customer_city','campinas')
    filterd_count = filterd_df.count()
    assert filterd_count == 1444
