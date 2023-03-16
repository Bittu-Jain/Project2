package pack

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object obj1 {

	def main(args: Array[String]): Unit = {

			val conf = new SparkConf().setAppName("Gitpractice").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val avro1 = spark.read.format("com.databricks.spark.avro")
					.load("file:///C://Project2/projectsample.avro")

					val parquet1 = spark.read.format("parquet")
					.load("file:///C://Project2/tempdest")

					val remnum = parquet1.withColumn("username",regexp_replace(col("username"),"[0-9]",""))

					val Brodjoin = avro1.join(broadcast(remnum),Seq("username"),"left")


					val notAvailableCustomer = Brodjoin.filter(col("nationality").isNull)
					
					val availableCustomer=Brodjoin.filter(col("nationality").isNotNull)
					
					val rep_string = notAvailableCustomer.na.fill("Not Available")
					val rep_int  = rep_string.na.fill(0)
					
					val notAvailableCustomer_currenttime = rep_int.withColumn("Current_Time",current_date)
					
					val AvailableCustomer_currenttime = availableCustomer.withColumn("Current_Time",current_date)
					
					notAvailableCustomer_currenttime.write.format("parquet").mode("overwrite").partitionBy("Current_Time")
					.save("file:///C://project2/dest/NotAvailableCustomer_currenttime")

					AvailableCustomer_currenttime.write.format("parquet").mode("overwrite").partitionBy("Current_Time")
					.save("file:///C://project2/dest/AvailableCustomer_currenttime")

					println("Done")
	}

}