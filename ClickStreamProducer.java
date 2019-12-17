package com.kindredgroup;


import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

/**
 * Created by rahumathulla on 12/14/2019. Mocking up the avro message producer.
 */

    public class ClickStreamProducer {



        static Logger logger = Logger.getLogger(ClickStreamProducer.class);


    static String[] jurisdiction = {"MT","UK","SJ","IT","FR","VS","FE","DK"};
        static String[] channel = {"WEB","PHONE","MOBILE","NATIVE"};
        static String[] brand = {"UNIBET","MARIA","RED","STANJAMES"};
        static long[] status = {200,404,403,500};


        public static void main(String[] args) throws IOException,InterruptedException  {
            logger.info("Setting Hadoop Capabilities");
            //Setting the hadoop environment for windows through code.
            System.setProperty("hadoop.home.dir","D:\\hadoop" );
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
            logger.info("Parsing Avro Schema");
            //Parsing the avro format file into avro schema
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(CommonUtils.getAvroSchema(avscFile));
            Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
            logger.info("Kafka properties initialization");
            //Configuring the properties for Kafka Producer
            Properties props = new Properties();
            props.put("bootstrap.servers", propertyParams.getKafkaBootServer().toString());
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

            KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

            for (int i = 1000; i < 12000; i++) {

                String eventJurisdiction="";
                String eventChannel="";
                String eventBrand ="";

                //Setting Default values if the Random string comes as NULL
                if(CommonUtils.getRandomString(jurisdiction)!=null){
                    eventJurisdiction = CommonUtils.getRandomString(jurisdiction);
                }else{
                    eventJurisdiction="UK";
                }

                if(CommonUtils.getRandomString(channel)!=null){
                    eventChannel = CommonUtils.getRandomString(channel);
                }else{
                    eventChannel="WEB";
                }

                if( CommonUtils.getRandomString(brand)!=null){
                    eventBrand =  CommonUtils.getRandomString(brand);
                }else{
                    eventBrand="MARIA";
                }



                Date eventDate = new Date();
                long eventTimestamp = eventDate.getTime();
                logger.info("Producing click stream records");
                GenericData.Record clickstreamRecord = new GenericData.Record(schema);
                clickstreamRecord.put("id", new Long(i));
                clickstreamRecord.put("name", "click-data-"+i);
                clickstreamRecord.put("eventTimestamp",eventTimestamp);
                clickstreamRecord.put("status", new Long(CommonUtils.getRandomLong(status)));
                clickstreamRecord.put("customerId",new Long(CommonUtils.genCustomerID()));
                clickstreamRecord.put("internalSystemInfo", propertyParams.getInternalSystemInfo().toString());


                GenericEnumSymbol jurisdictionSymbol = new GenericData.EnumSymbol(schema.getField("jurisdiction").schema().getTypes().get(1),eventJurisdiction);
                GenericEnumSymbol channelSymbol = new GenericData.EnumSymbol(schema.getField("channel").schema().getTypes().get(1), eventChannel);
                GenericEnumSymbol brandSymbol = new GenericData.EnumSymbol(schema.getField("brand").schema().getTypes().get(1), eventBrand);

                clickstreamRecord.put("jurisdiction", jurisdictionSymbol);
                clickstreamRecord.put("channel", channelSymbol);
                clickstreamRecord.put("brand",brandSymbol);

                if(clickstreamRecord!=null) {
                    byte[] streamBytes = recordInjection.apply(clickstreamRecord);
                    ProducerRecord<String, byte[]> streamRecord = new ProducerRecord<>(propertyParams.getKafkaRawTopic().toString(), streamBytes);
                    producer.send(streamRecord);
                    System.out.println(i + "," + "click-data-" + i + "," + eventTimestamp+","+eventJurisdiction+","+eventChannel+","+eventBrand);
                }
                Thread.sleep(100);
            }
        }

}
