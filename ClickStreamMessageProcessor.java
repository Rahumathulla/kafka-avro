package com.kindredgroup;

/**
 * Created by rahumathulla on 12/16/2019.
 */

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import java.io.IOException;
import org.apache.spark.sql.avro.package$;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import static org.apache.spark.sql.functions.*;

/**
 * Spark Consumer to Process Avro Stream From Kafka Topic and publish the processed data (Mobile) to next layer
 */
public class ClickStreamMessageProcessor {
    static Logger logger = Logger.getLogger(ClickStreamMessageProcessor.class);
    public static void main(String[] args) throws IOException, StreamingQueryException {
        PropertyParams propertyParams = null;
        //Reading the property file
        PropertyUtil propertyUtil =  new PropertyUtil();
        try {
            propertyParams = propertyUtil.getProperties("C:\\Users\\rahumathulla\\IdeaProjects\\kindred-message-stream\\src\\main\\resources\\config.properties");
            logger.info("Property values have been loaded");
        } catch (Exception e) {
            e.printStackTrace();
        }

        String avscFile = propertyParams.getAvroSchemaFile().toString();
        String boot_servers = propertyParams.getKafkaBootServer().toString();
        System.setProperty("hadoop.home.dir","D:\\hadoop" );

        SparkSession sparkSession = SparkSession
                .builder().master("local")
                .getOrCreate();

        sparkSession.conf().set("spark.sql.streaming.checkpointLocation",propertyParams.getSparkCheckPoint().toString());
        logger.info("Consuming messages from the topic: "+propertyParams.getKafkaRawTopic().toString());
        Dataset<Row> clickstream = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", boot_servers)
                .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .option("subscribe", propertyParams.getKafkaRawTopic().toString())
                .load();

        Dataset<Row> moobilestream= clickstream.select(package$.MODULE$.from_avro(col("value"), CommonUtils.getAvroSchema(avscFile)).as("clickdata"))
                .select("clickdata.*").where(col(propertyParams.getFilterName().toString()).equalTo(propertyParams.getFilterValue().toString()));

        //CSV or Json - Avro
        Dataset<Row> streamdata = moobilestream.select(concat(
                col("id"),
                lit(propertyParams.getDelimiter().toString()),col("name"),
                lit(propertyParams.getDelimiter().toString()),col("eventTimestamp"),
                lit(propertyParams.getDelimiter().toString()),col("status"),
                lit(propertyParams.getDelimiter().toString()),col("internalSystemInfo"),
                lit(propertyParams.getDelimiter().toString()),col("jurisdiction"),
                lit(propertyParams.getDelimiter().toString()),col("channel"),
                lit(propertyParams.getDelimiter().toString()),col("brand")).as("value"));

        //System.out.println("-"+streamdata.toString());
        logger.info("Writing filtered messages to the topic: "+propertyParams.getKafkaMobileTopic().toString());
        StreamingQuery processQuery  = streamdata
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", boot_servers)
                .option("topic", propertyParams.getKafkaMobileTopic().toString())
                .start();

        processQuery.awaitTermination();


    }
}


