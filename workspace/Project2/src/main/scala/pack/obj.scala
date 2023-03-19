package pack

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.io.Source

object obj {

	def main(args: Array[String]): Unit = {

			val conf = new SparkConf().setAppName("Gitpractice").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._


					val avro1 = spark.read.format("com.databricks.spark.avro").load("file:///home/cloudera/projectsample.avro")

					val html = Source.fromURL("https://randomuser.me/api/0.8/?results=10").mkString

					val urlrdd = sc.parallelize(List(html))

					val df = spark.read.json(urlrdd)


					val procdf = df.withColumn("results",explode(col("results")))


					val flattendata  = procdf.select(
							"nationality",
							"seed",
							"version",
							"results.user.cell",
							"results.user.dob",
							"results.user.email",
							"results.user.gender",
							"results.user.location.*",
							"results.user.md5",
							"results.user.name.*",
							"results.user.password",
							"results.user.phone",
							"results.user.picture.*",
							"results.user.registered",
							"results.user.salt",
							"results.user.sha1",
							"results.user.sha256",
							"results.user.username"
							)



					flattendata.write.format("parquet").mode("overwrite").option("header", "true")
					.save("hdfs:/user/cloudera/tempdest")




	}

}