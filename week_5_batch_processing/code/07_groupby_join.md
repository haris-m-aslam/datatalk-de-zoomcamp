```python
import pyspark
from pyspark.sql import SparkSession
```


```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```

    WARNING: An illegal reflective access operation has occurred
    WARNING: Illegal reflective access by org.apache.spark.unsafe.Platform (file:/home/haris/spark/spark-3.0.3-bin-hadoop3.2/jars/spark-unsafe_2.12-3.0.3.jar) to constructor java.nio.DirectByteBuffer(long,int)
    WARNING: Please consider reporting this to the maintainers of org.apache.spark.unsafe.Platform
    WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
    WARNING: All illegal access operations will be denied in a future release
    22/02/28 16:14:39 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
    Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).



```python
df_green = spark.read.parquet('data/pq/green/*/*')
```


```python
df_green.registerTempTable('green')
```


```python
df_green_revenue.write.parquet('data/report/revenue/green')
```

                                                                                    


```python
df_green_revenue = spark.sql("""
SELECT 
    date_trunc('hour', lpep_pickup_datetime) AS hour, 
    PULocationID AS zone,

    SUM(total_amount) AS amount,
    COUNT(1) AS number_records
FROM
    green
WHERE
    lpep_pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2
""")
```


```python
df_green_revenue \
    .repartition(20) \
    .write.parquet('data/report/revenue/green', mode='overwrite')
```

                                                                                    


```python
df_yellow = spark.read.parquet('data/pq/yellow/*/*')
df_yellow.registerTempTable('yellow')
```


```python
df_yellow_revenue = spark.sql("""
SELECT 
    date_trunc('hour', tpep_pickup_datetime) AS hour, 
    PULocationID AS zone,

    SUM(total_amount) AS amount,
    COUNT(1) AS number_records
FROM
    yellow
WHERE
    tpep_pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2
""")
```


```python
df_yellow_revenue \
    .repartition(20) \
    .write.parquet('data/report/revenue/yellow', mode='overwrite')
```

                                                                                    


```python
df_green_revenue = spark.read.parquet('data/report/revenue/green')
df_yellow_revenue = spark.read.parquet('data/report/revenue/yellow')
```


```python
df_join = df_green_revenue.join(df_yellow_revenue, on=['hour', 'zone'], how='outer')
```


```python
df_join.show()
```

    [Stage 14:======================================>                   (2 + 1) / 3]

    +-------------------+----+------+--------------+------------------+--------------+
    |               hour|zone|amount|number_records|            amount|number_records|
    +-------------------+----+------+--------------+------------------+--------------+
    |2020-01-01 00:00:00|   3|  null|          null|              25.0|             1|
    |2020-01-01 01:00:00|  17|598.15|            18|            464.51|            18|
    |2020-01-01 01:00:00| 107|  null|          null| 9994.480000000021|           583|
    |2020-01-01 01:00:00| 162|  null|          null| 5736.690000000002|           298|
    |2020-01-01 02:00:00| 234|  null|          null| 6759.990000000009|           370|
    |2020-01-01 03:00:00| 170|  null|          null|            4632.0|           257|
    |2020-01-01 04:00:00|  22|  null|          null|             12.96|             1|
    |2020-01-01 06:00:00| 255|121.12|             3|36.260000000000005|             3|
    |2020-01-01 07:00:00| 235| 32.95|             2|              null|          null|
    |2020-01-01 09:00:00|  70|  null|          null|              54.9|             3|
    |2020-01-01 10:00:00| 181| 54.56|             5|             70.45|             2|
    |2020-01-01 13:00:00| 224|  null|          null|272.71000000000004|            18|
    |2020-01-01 15:00:00| 123| 35.71|             2|              null|          null|
    |2020-01-01 18:00:00|  68|  null|          null| 3645.649999999997|           213|
    |2020-01-01 19:00:00| 100|  null|          null| 2726.739999999997|           175|
    |2020-01-01 23:00:00| 234|  null|          null|1388.0899999999997|            88|
    |2020-01-02 00:00:00| 219|  null|          null|              61.3|             1|
    |2020-01-02 02:00:00|   7|  10.3|             1| 66.89999999999999|             4|
    |2020-01-02 04:00:00|  36| 33.36|             2|              null|          null|
    |2020-01-02 05:00:00|   7| 54.22|             2|            212.11|            11|
    +-------------------+----+------+--------------+------------------+--------------+
    only showing top 20 rows
    


                                                                                    


```python
df_green_revenue_tmp = df_green_revenue \
    .withColumnRenamed('amount', 'green_amount') \
    .withColumnRenamed('number_records', 'green_number_records')

df_yellow_revenue_tmp = df_yellow_revenue \
    .withColumnRenamed('amount', 'yellow_amount') \
    .withColumnRenamed('number_records', 'yellow_number_records')
```


```python
df_join = df_green_revenue_tmp.join(df_yellow_revenue_tmp, on=['hour', 'zone'], how='outer')
```


```python
df_join.write.parquet('data/report/revenue/total', mode='overwrite')
```

                                                                                    


```python
df_join = spark.read.parquet('data/report/revenue/total')
```


```python
df_zones = spark.read.parquet('zones/')
```


```python
df_result = df_join.join(df_zones, df_join.zone == df_zones.LocationID)
```


```python
df_result.drop('LocationID', 'zone').write.parquet('tmp/revenue-zones')
```

                                                                                    
