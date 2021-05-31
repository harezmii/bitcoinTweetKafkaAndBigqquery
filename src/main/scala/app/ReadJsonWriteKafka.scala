package app

import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.{SparkSession, types}


object ReadJsonWriteKafka {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("readJson") getOrCreate()
    val readJsonData = sparkSession.read.json("/home/harezmi/Desktop/2010-01-012021-05-29.json")
    val selectData = readJsonData.select("created_at", "tweet")



    val castData = selectData.select(to_json(struct("*")).as("value"))
      .selectExpr("CAST(value AS STRING)")

    castData.write
      .format("kafka")
      .option("kafka.bootstrap.servers", "35.223.195.127:9092")
      .option("topic","tweet")
      .save()

  }
}
