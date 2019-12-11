import os
import sys
import pyspark
import string
import csv
import json
import statistics
from itertools import combinations
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql import types as D
from pyspark.sql.window import Window
from dateutil.parser import parse
import datetime

spark = SparkSession.builder.appName("project-part1").config("spark.some.config.option", "some-value").getOrCreate()

if not os.path.exists('Results_JSON'):
    os.makedirs('Results_JSON')

files=os.listdir('NYCOpenData/')
files_list=[]
files_dict={}
with open('NYCOpenData/datasets.tsv') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    for i,row in enumerate(reader):
        files_dict[row[0].lower()]=row[1]
        files_list.append(row[0]+'.tsv.gz')

files.remove('datasets.tsv')
files_list=files_list[580:]
def count_not_null(c, nan_as_null=False):
    pred = F.col(c).isNotNull() & (~isnan(c) if nan_as_null else F.lit(True))
    return F.sum(pred.cast("integer")).alias(c)


def validate_string_to_integer(d):
    if type(d)==str:
        try:
            z=int(d)
            return z
        except:
            return None
    else:
        return None
    
def validate_string_to_float(d):
    if type(d)==str:
        try:
            z=float(d)
            return z
        except:
            return None
    else:
        return None

def validate_date(d):
    try:
        z=parse(d)
        return str(z)
    except:
        return None


get_int=F.udf(lambda x: x if type(x)==int else None, D.IntegerType())
get_str=F.udf(lambda x: x if type(x)==str else None, D.StringType())
get_flt=F.udf(lambda x: x if type(x)==float else None, D.FloatType())
get_dt=F.udf(lambda x: validate_date(x), D.StringType())
get_string_int=F.udf(lambda x: validate_string_to_integer(x), D.IntegerType())
get_string_flt=F.udf(lambda x: validate_string_to_float(x), D.FloatType())

c=1

print('files left = ', len(files_list)) 
for file in files_list:
    filepath='/user/hm74/NYCOpenData/'+file.lower()+'.tsv.gz'
    DF = spark.read.format('csv').options(header='true',inferschema='true').option("delimiter", "\t").load(filepath)
    DF_dict={"dataset_name": files_dict[file.split('.')[0]], 'columns': DF.columns, 'key_column_candidates':[]}
    columns_names=DF.columns 
    cols_data=[]
    total_rows=DF.count()
    
    for i, x in enumerate(DF.columns):
        DF=DF.withColumnRenamed(x, str(i))
    compute_not_null_columns = DF.agg(*[count_not_null(c) for c in DF.columns]).collect()[0]
    computer_null_columns=[(total_rows-count_notNull) for count_notNull in compute_not_null_columns]
    
     
    for i, cols in enumerate(DF.columns):
        if total_rows==0:
            continue
        columns_data={}
        columns_data["column_name"]=columns_names[i]
        columns_data['number_non_empty_cells']=compute_not_null_columns[i]
        columns_data['number_empty_cells']=computer_null_columns[i]
        frequency_dataframe=DF.groupBy(cols).count().sort(F.desc('count'))
        frequency_dataframe=frequency_dataframe.where(F.col(cols).isNotNull())
        
        top_frequency_five=[]
        if frequency_dataframe.count()<5:
            top_frequency_five=[row[0] for row in frequency_dataframe.collect()]
        else:
            top_frequency_five=[row[0] for row in frequency_dataframe.take(5)]
        columns_data['frequent_values']=top_frequency_five
        columns_data['data_types']=[]
        
        int_col=cols+' '+'int_type'
        str_col=cols+' '+'str_type'
        float_col=cols+ ' '+ 'float_type'
        date_col=cols+' '+'date_type'
        str_int_col=cols + ' '+'str_int'
        str_float_col=cols +' '+'str_float'
        df=DF.select([get_int(cols).alias(int_col), get_str(cols).alias(str_col), get_flt(cols).alias(float_col), get_dt(cols).alias(date_col),
                     get_string_int(cols).alias(str_int_col),get_string_flt(cols).alias(str_float_col)])
        
        int_df=df.select(int_col).where(F.col(int_col).isNotNull())
        str_df=df.select(str_col).where(F.col(str_col).isNotNull())
        float_df=df.select(float_col).where(F.col(float_col).isNotNull())
        date_df=df.select(date_col).where(F.col(date_col).isNotNull())
        str_int_df=df.select(str_int_col).where(F.col(str_int_col).isNotNull())
        str_float_df=df.select(str_float_col).where(F.col(str_float_col).isNotNull())
        
        if float_df.count()>1:
            type_data={}
            type_data['type']='REAL'
            type_data['count']=float_df.count()
            type_data['max_value']=float_df.agg({float_col: "max"}).collect()[0][0]
            type_data['min_value']=float_df.agg({float_col: "min"}).collect()[0][0]
            type_data['mean']=float_df.agg({float_col: "avg"}).collect()[0][0]
            type_data['stddev']=float_df.agg({float_col: 'stddev'}).collect()[0][0]
            columns_data['data_types'].append(type_data)
        
        if int_df.count()>1:
            type_data={}
            type_data['type']='INTEGER (LONG)'
            type_data['count']=int_df.count()
            type_data['max_value']=int_df.agg({int_col: 'max'}).collect()[0][0]
            type_data['min_value']=int_df.agg({int_col: 'min'}).collect()[0][0]
            type_data['mean']=int_df.agg({int_col: 'avg'}).collect()[0][0]
            type_data['stddev']=int_df.agg({int_col: 'stddev'}).collect()[0][0]
            columns_data['data_types'].append(type_data)
            
        if str_df.count()>1:
            type_data={'type':'TEXT', 'count': str_df.count()}
            str_rows=str_df.distinct().collect()
            str_arr=[row[0] for row in str_rows]
            if len(str_arr)<=5:
                type_data['shortest_values']=str_arr
                type_data['longest_values']=str_arr
                
            else:
                str_arr.sort(key=len, reverse=True)
                type_data['shortest_values']=str_arr[-5:]
                type_data['longest_values']=str_arr[:5]
            
            type_data['average_length']=sum(map(len, str_arr))/len(str_arr)
            columns_data['data_types'].append(type_data)
        
        if date_df.count()>1:
            type_data={"type":"DATE/TIME", "count":date_df.count()}
            min_date, max_date = date_df.select(F.min(date_col), F.max(date_col)).first()
            type_data['max_value']=max_date
            type_data['min_value']=min_date
            columns_data['data_types'].append(type_data)
        
        if str_float_df.count()>1:
            type_data={}
            type_data['type']='REAL'
            type_data['count']=str_float_df.count()
            type_data['max_value']=str_float_df.agg({str_float_col: "max"}).collect()[0][0]
            type_data['min_value']=str_float_df.agg({str_float_col: "min"}).collect()[0][0]
            type_data['mean']=str_float_df.agg({str_float_col: "avg"}).collect()[0][0]
            type_data['stddev']=str_float_df.agg({str_float_col: 'stddev'}).collect()[0][0]
            columns_data['data_types'].append(type_data)
        
        if str_int_df.count()>1:
            type_data={}
            type_data['type']='INTEGER (LONG)'
            type_data['count']=str_int_df.count()
            type_data['max_value']=str_int_df.agg({str_int_col: 'max'}).collect()[0][0]
            type_data['min_value']=str_int_df.agg({str_int_col: 'min'}).collect()[0][0]
            type_data['mean']=str_int_df.agg({str_int_col: 'avg'}).collect()[0][0]
            type_data['stddev']=str_int_df.agg({str_int_col: 'stddev'}).collect()[0][0]
            columns_data['data_types'].append(type_data)
        cols_data.append(columns_data)
    
    
    output_file=file.split('.')[0]
    print('Processed '+ str(c)+' file')
    c=c+1
    output_file='Results_JSON/'+ output_file
    with open(output_file, 'w', newline='\n') as json_file:
        json.dump(DF_dict, json_file)
        for Dict in cols_data:
            json.dump(Dict, json_file,default=str)
    
    
            
      


