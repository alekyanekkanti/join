import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.databricks.spark.avro

object Csv {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL example")
      .master("local")
      .getOrCreate()
    import spark.implicits._


    val data = loadTrainingData(spark)
    data.toDF.createOrReplaceTempView("dataTB1")
     spark.sql("SELECT * FROM  dataTB1")
    data.printSchema()

    val result = data.toDF().show()

    val temp = TrainingData(spark)
    temp.toDF.createOrReplaceTempView("dataTB2")
    spark.sql("SELECT * FROM  dataTB2")
    temp.printSchema()

    val join = spark.sql("SELECT b.ename,a.empid,a.managername FROM dataTB1 a INNER JOIN dataTB2 b ON a.empid = b.empid AND a.noofdays>4")
    join.show()
   join.write.mode("overwrite").parquet("C:/alekya/files")

  }
    def loadTrainingData(spark: SparkSession): DataFrame = {
      val df1 = spark
        .read.format("com.databricks.spark.avro")
        .option("header", "true")
        .load("C:/alekya/svfiles/avro").na.drop()

      df1
    }
  def TrainingData(spark: SparkSession): DataFrame = {
    val df1 = spark
      .read.format("com.databricks.spark.avro")
      .option("header", "true")
      .load("C:/alekya/csvfiles/avro").na.drop()

    df1
  }



}