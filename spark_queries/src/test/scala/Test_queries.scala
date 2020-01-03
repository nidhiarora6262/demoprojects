
import Demo_project.spark_queries
import org.apache
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import Demo_project.spark_queries.spark
//import org.testng.Assert._


class Test_queries extends FunSuite {
  var spark: SparkSession = _

  //val data = spark.read.option("header", "true").csv("/home/nidhi/Documents/datanew.csv")
  def beforeEach() {
    spark = new SparkSession.Builder().appName("Spark Job for Loading Data").master("local[*]").getOrCreate()
  }

  test("testing RDD") {
    val data1 = spark.read.option("header", "true").csv("/home/nidhi/Desktop/FL_insurance_sample.csv")
    assert(data1.count() != 0)
  }
}

/*

  test("casting") {

    val abc = spark_queries.schema1("1","80","123" )
    assert (data.schema != abc.schema)
  }

  test("groupBY") {

    val ans = spark_queries groupcount("Nidhi", "bca")
    assert(ans.count() === 9)

  }



  test("distinct")
  {
    val ans1=spark_queries.name("Nidhi")
    assert(ans1.count() > 1)
  }


  test("sum") {
    val ans = spark_queries.sum(2, 3)
    assert(ans === 5)

  }
}

*/
