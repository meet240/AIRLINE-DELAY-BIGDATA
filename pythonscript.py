#read file from S3 into dataframe
df=spark.read.csv(s3suyash.proxairline_delay_analysis,header=True)

#drop unwanted columns (null  90%)
df=df.drop(Unnamed 27)
df=df.drop(LATE_AIRCRAFT_DELAY)
df=df.drop(SECURITY_DELAY)
df=df.drop(NAS_DELAY)
df=df.drop(WEATHER_DELAY)
df=df.drop(CARRIER_DELAY)
df=df.drop(TAXI_IN)
df=df.drop(TAXI_OUT)
df=df.drop(CANCELLATION_CODE)
df=df.drop(WHEELS_OFF)
df=df.drop(WHEELS_ON)

#count number of null and missing values
from pyspark.sql.functions import isnan,when,count,col
df.select([count(when(isnan(c)col(c).isNull(),c)).alias(c) for c in df.columns]).show()

#drop rows with null values
df=df.where(col(ARR_DELAY).isNotNull())
df=df.where(col(CRS_DEP_TIME).isNotNull())
df=df.where(col(ACTUAL_ELAPSED_TIME).isNotNull())

#cleaning data format hhmm - hhmmss (shown for one column)
df=df.withColumn(temp1,concat(lit(00),col(CRS_ARR_TIME)))
df=df.withColumn(col2,F.split(col(temp1),.).getItem(0))
df=df.withColumn(col3,df.col2.substr(-4,4))
df=df.withColumn(col4,df.col3.substr(1,2))
df=df.withColumn(col5,df.col4.substr(-2,2))
df=df.withColumn(semi,F.concat(F.col(col4),F.lit(),F.col(col5)))
df=df.withColumn(CRS_DEP_TIME,F.concat(F.col(semi),F.lit(),F.col(col6)))

#Converting tbles to TimeStamp
df=df.withColumn(temp1,concat(col(FL_DATE),lit( ),col(CRS_ARR_TIME)))  
df=df.withColumn(CRS_ARR_TIME,col(temp1).cast(timestamp))  
df=df.withColumn(temp1,concat(col(FL_DATE),lit( ),col(DEP_TIME)))  
df=df.withColumn(DEP_TIME,col(temp1).cast(timestamp))  
df=df.withColumn(temp1,concat(col(FL_DATE),lit( ),col(ARR_TIME)))  
df=df.withColumn(ARR_TIME,col(temp1).cast(timestamp))  
df=df.withColumn(temp1,concat(col(FL_DATE),lit( ),col(CRS_DEP_TIME)))  
df=df.withColumn(CRS_DEP_TIME,col(temp1).cast(timestamp))  

#type casting columns
df=df.withColumn(SECURITY_DELAY,df.SECURITY_DELAY.cast('int')) 
df=df.withColumn(WEATHER_DELAY,df.WEATHER_DELAY.cast('int')) 
df=df.withColumn(NAS_DELAY,df.NAS_DELAY.cast('int')) 
df=df.withColumn(LATE_AIRCRAFT_DELAY,df.LATE_AIRCRAFT_DELAY.cast('int')) 

#dump dataframe in parquet format in s3
df.write.parquet(s3suyash.proxpreprocessclean1)
df.read.parquet(s3suyash.proxpreprocessclean1)
df=spark.read.parquet(s3suyash.proxpreprocessclean1)

#process incremental data
#read data into dataframe
df=spark.read.csv(s3suyashpro2020,header=True)

#add an index column for splitting the dataframe 
from pyspark.sql.functions import monotonically_increasing_id
df=df.withColumn(Index,monotonically_increasing_id())
df = df.drop(Unnamed 27)

#split dataframe into two subsets (load and incremental)
from pyspark.sql.functions import col
df1=df.filter(col(Index)  1273787)
df2=df.filter(col(Index) = 1273787)

#drop the index value after split
df1=df1.drop(Index)
df2=df2.drop(Index)

#get the SQL context
from pyspark.sql import SQLContext
sqlcontext=SQLContext(sc)

#registering dataframe and creating HIVE tables
df.createOrReplaceTempView(demo)
sqlcontext.sql(create table airline as select  from demo)
df2.createOrReplaceTempView(demo2)
sqlcontext.sql(create table airline_inc as select  from demo2)

