#This notebook will contain function for all the transformations.
from pyspark.sql import functions as F
def renameCol(df,old_name,new_name):
    df = df.withColumnRenamed(f'{old_name}',f'{new_name}')
    return df

def dropCol(df,colList):
    df = df.drop(*colList)
    return df

def joinDF(df1,df2,col1,col2,join_type):
    #df = df1.join(df2,F.col(f'col1') == F.col('col2'),join_type)
    df = df1.alias('left').join(df2.alias('right'),F.col(f'left.{col1}') == F.col(f'right.{col2}'),join_type)
    #df = df1.join(df2, df1[col1]== df2[col2],join_type)
    return df

def aggregation(df,col_list,col1):
    df = df.groupBy(*col_list).agg(F.sum(F.col(f'{col1}')).alias('total_price_per_order'),F.sum(F.col('freight_value')).alias('total_freight_per_order'))
    return df

def selectCol(df,col_list):
    return df.select(*col_list)

def filterDf(df,col_name,value):
    df = df.filter(f'{col_name} == "{value}"')
    return df