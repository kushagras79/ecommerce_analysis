#This is the master file.
from lib import utils,dataReader,dataTransformation,dataWriter
if __name__ == "__main__":
    print('creating spark session')
    spark = utils.getSparkSession()
    print('spark session is created.')
    #getting customers data
    cust_df = dataReader.read_customers()
    cust_df = dataTransformation.renameCol(cust_df,'customer_zip_code_prefix','customer_zip_code')
    #gettung sellers information.
    sellersDf = dataReader.read_seller()
    sellersDf = dataTransformation.renameCol(sellersDf,'seller_zip_code_prefix','seller_zip_code')
    #joining cust and seller to get customer and seller information as single file
    joined_df = dataTransformation.joinDF(cust_df,sellersDf,'customer_zip_code','seller_zip_code','inner')
    joined_df = dataTransformation.renameCol(joined_df,'customer_zip_code','zip_code')
    joined_df = dataTransformation.renameCol(joined_df,'customer_city','city')
    joined_df = dataTransformation.renameCol(joined_df,'customer_state','state')
    joined_df = dataTransformation.dropCol(joined_df,['seller_zip_code','seller_city','seller_state'])
    #dataWriter.writeDf(joined_df,'data/curated/customer_sellers/')

    # reading orders data.
    orders_df = dataReader.read_orders()
    orders_df = dataTransformation.dropCol(orders_df,['order_purchase_timestamp','order_approved_at','order_delivered_carrier_date'])
    dataWriter.writeDf(orders_df,'data/curated/orders/')
    #reading orders_items data
    orders_items = dataReader.read_orders_item()
    orders_items = dataTransformation.dropCol(orders_items,['shipping_limit_date'])
    dataWriter.writeDf(orders_items,'data/curated/orders_items/')
    #reading products data
    products_df = dataReader.read_products()
    products_df = dataTransformation.selectCol(products_df,['product_id','product_category_name'])

    products_categroy_df = dataReader.read_products_category()
    products_df = dataTransformation.joinDF(products_df,products_categroy_df,'product_category_name','product_category_name','inner')
    products_df = dataTransformation.dropCol(products_df,['product_category_name'])
    products_df = dataTransformation.renameCol(products_df,'product_category_name_english','product_category_name')
    dataWriter.writeDf(products_df,'data/curated/products/')
    spark.stop()