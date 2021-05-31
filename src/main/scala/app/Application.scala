package app

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{base64, col, from_json, window}
import org.apache.spark.sql.types.{DataTypes, StructType}
import com.google.cloud.spark.bigquery
import org.apache.hadoop.conf.Configuration



object Application {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("app").master("local").getOrCreate()
    val rawData = sparkSession
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "35.223.195.127:9092")
      .option("subscribe", "tweet")
      .load()
    val context =  sparkSession.sparkContext
    val configure = context.hadoopConfiguration;

    configure.set("google.cloud.auth.service.account.enable", "true");
    configure.set("google.cloud.auth.service.account.email", "test-779@kafka-315320.iam.gserviceaccount.com");
    configure.set("google.cloud.auth.service.account.keyfile", "/home/harezmi/Desktop/twitterDataGetKafkaAnalyzeSpark/src/main/scala/file/kafka-315320-1056bac862eb.p12");


    val schema = new StructType()
      .add("created_at", DataTypes.TimestampType)
      .add("tweet", DataTypes.StringType)

    val dataAll = rawData.select("value").selectExpr("CAST(value as STRING)")
      .select(from_json(col("value"), schema)).select("from_json(value).*")

    val resultData = dataAll.groupBy(window(col("created_at"), "1 hour")).count()

    // token süresi dolduğunda refresh olmalı
    val token = "ya29.a0AfH6SMBZ0BSbTGUK9iY5o03SctYRoHh-_VljVlqvLYJOgsxFK2zvPCETFLOcqMCn1yWaPbIDPVCDHef3rF6SNfo-g4ZqnBdMPXLkbQ5BBb--Ortyoo034Dcpk0EcXNuL9DOmWwzFyNnJuQRFGNo4CGg_aa7mGQzx_8YxLNfuyrgDbIJ_det45Xsr57ojuPGKCrApPxMfmMyGwk_bfnia2-BAwYxpCGp8a7AGyTWUOpRigGXu66gH63QfK3WgoLPQv-BVLkc"

    sparkSession.conf.set("gcpAccessToken", token)
    resultData.write
      .format("bigquery")
      .mode("append")
      .option("temporaryGcsBucket", "kafkatobigque")
      .option("project", "kafka-315320")
      .option("parentProject", "kafka-315320")
      .option("table","kafka-315320.yeni.tweet")
      .save()
  }
}