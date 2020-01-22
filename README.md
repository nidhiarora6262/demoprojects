##demo_projects

ScalaVersion := "2.11.12"

Spark version :="2.4.0"

Scala Test :="3.0.8"

This task is about creating a range frame and performing all aggregations on that group.

1.###To read the data through in built read function .
~~~
spark.read.option("header", "true").csv("/home/nidhi/Desktop/FL_insurance_sample.csv")
~~~
2.Using  window function 

~~~
val window = Window.partitionBy("DEVICE_ID").orderBy("TIMESTAMP")
~~~
3.Use lag and lead function to calculate previous and next columns on flow_rate.

4.Make new columns according to conditions given  using  previous and next columns
~~~
df.withColumn("start_flowrate", functions.when(functions.col("previous") > 0.0D and functions.col("next") >= 0.0D, 0).otherwise(1.0))
      .withColumn("end_flowrate", functions.when(functions.col("next") equalTo 0.0D and functions.col("previous") > 0.0D, 0).otherwise(1.0))
~~~

5.Make one new column also using lag function previousstartflowrate and previousendflowrate.

6.Then make column startimestamp comparing previousstartflowrate and previousendflowrate where condition statisfy put the timestamp value. 

~~~
df6.withColumn("endtimestamp", functions.when(functions.col("end_flowrate") equalTo 0.0D and functions.col("previousendflowrate") > 0.0D, df2.col("TIMESTAMP")))
~~~
7.Make another window for placing last and first value of group.


