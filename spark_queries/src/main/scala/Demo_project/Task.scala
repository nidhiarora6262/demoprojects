package Demo_project
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.storage.StorageLevel

  object Task {

    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()

        .master("local[*]")

        .appName("Insurance_file")

        .config("spark.some.config.option", "some-value")

        .getOrCreate()

      // query 1read the csv file
      val data1 = spark.read.option("header", "true").csv("/home/nidhi/Desktop/FL_insurance_sample.csv")

      //query 2print the schema of file
      spark.time(data1.printSchema())

      // query 2show the data of the file
      spark.time(data1.show())
      data1.persist(StorageLevel.MEMORY_AND_DISK)
      //data1.unpersist

      //query 3 casting of columns into Doubletype
      val data2 = data1.withColumn("eq_site_limit", data1("eq_site_limit").cast(DoubleType))
        .withColumn("hu_site_limit", data1("hu_site_limit").cast(DoubleType))
        .withColumn("fl_site_limit", data1("fl_site_limit").cast(DoubleType))
        .withColumn("tiv_2011,", data1("tiv_2011").cast(DoubleType))
        .withColumn("tiv_2012", data1("tiv_2012").cast(DoubleType))
        .withColumn("eq_site_deductible", data1("eq_site_deductible").cast(DoubleType))
        .withColumn("hu_site_deductible", data1("hu_site_deductible").cast(DoubleType))
        .withColumn("fl_site_deductible", data1("fl_site_deductible").cast(DoubleType))
        .withColumn("fr_site_deductible", data1("fr_site_deductible").cast(DoubleType))
      data2.printSchema()


      // query 4 unpivot the data
      val DF4 = data1.selectExpr("policyId", "statecode", "county", "construction", "point_latitude", "point_longitude", "stack(10, 'eq_site_limit',eq_site_limit, 'hu_site_limit',hu_site_limit, 'fl_site_limit',fl_site_limit, 'fr_site_limit',fr_site_limit,'tiv_2011',tiv_2011,' tiv_2012',tiv_2012, 'eq_site_deductible',eq_site_deductible,' hu_site_deductible', hu_site_deductible,'fl_site_deductible',fl_site_deductible, 'fr_site_deductible', fr_site_deductible) as (LimitValue,Limitcode)").where("Limitcode is not null")
      DF4.show()
      spark.time(DF4.show())
      DF4.persist

      //query 5 count of limit_value for each policyID, statecode, county, limit_code and Add processing_datetime_utc column to output after step 5 and show results
      val df3 = DF4.groupBy("policyId", "statecode", "county", "Limitcode").agg(count("Limitvalue")).withColumn("processing_datetime_utc", lit((0))).show()

      //query 7 get the count of all records by grouping point_latitude and point_longitude on data retrieved
      val df7 = DF4.groupBy("point_longitude", "point_latitude").count().show()

      //Check how many point_latitude and point_longitude values are there for each policyID
      val df8 = data1.groupBy("policyID").agg(collect_list("point_longitude"), collect_list("point_latitude")).show()
    }





    /*def groupcount(Name:String,Stream:String):DataFrame= {
      val data7 = data.groupBy("Name", "Stream").count()
      data7
    }


    def sum(a: Int, b: Int): Int = {
      val data8 = a + b
      data8

    }

    def name(Name: String): DataFrame= {
      val name = data.select("Name").distinct()
      name
    }*/


  }




















