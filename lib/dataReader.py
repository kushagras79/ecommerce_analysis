#This notebook will contain functions to read the file and create a dataframe..
from pyspark.sql.types import *
from lib.utils import getSparkSession
def get_customers_schema():
    cust_schema = StructType([
        StructField('customer_id',StringType()),
        StructField('customer_unique_id',StringType()),
        StructField('customer_zip_code_prefix',IntegerType()),
        StructField('customer_city',StringType()),
        StructField('customer_state',StringType())
    ])
    return cust_schema
    
def get_geolocation_schema():
    geo_schema = StructType([
        StructField('geolocation_zip_code_prefix',IntegerType()),
        StructField('geolocation_lat',FloatType()),
        StructField('geolocation_lng',FloatType()),
        StructField('geolocation_city',StringType()),
        StructField('geolocation_state',StringType())
    ])
    return geo_schema

def get_seller_schema():
    seller_schema = StructType([
        StructField('seller_id',StringType()),
        StructField('seller_zip_code_prefix',IntegerType()),
        StructField('seller_city',StringType()),
        StructField('seller_state',StringType())
    ])
    return seller_schema

def get_orders_schema():
    orders_schema = StructType([
        StructField('order_id',StringType()),
        StructField('customer_id',StringType()),
        StructField('order_status',StringType()),
        StructField('order_purchase_timestamp',StringType()),
        StructField('order_approved_at',StringType()),
        StructField('order_delivered_carrier_date',StringType()),
        StructField('order_delivered_customer_date',StringType()),
        StructField('order_estimated_delivery_date',StringType())
        ])
    return orders_schema

def get_orders_items_schema():
    orders_items_schema = StructType([
        StructField('order_id',StringType()),
        StructField('order_item_id',StringType()),
        StructField('product_id',StringType()),
        StructField('seller_id',StringType()),
        StructField('shipping_limit_date',StringType()),
        StructField('price',FloatType()),
        StructField('freight_value',FloatType())
    ])
    return orders_items_schema


def get_products_schema():
    product_schema = 'product_id string,product_category_name string,product_name_lenght integer,product_description_lenght integer,product_photos_qty	integer, product_weight_g integer,	product_length_cm integer,product_height_cm integer,product_width_cm integer'
    return product_schema


def get_products_category_schema():
    return StructType([
            StructField('product_category_name',StringType()),
            StructField('product_category_name_english',StringType())
    ])

def create_df(mySchema,filePath):
    spark = getSparkSession()
    df = spark.read.format('csv').schema(mySchema).option('mode','permissive').\
        option('header','true').option('inferSchema','false').\
        load(f'{filePath}')
    return df
