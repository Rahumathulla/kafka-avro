package com.kindredgroup;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by rahumathulla on 12/16/2019.
 */
public class PropertyUtil {
    static Logger logger = Logger.getLogger(PropertyUtil.class);
    public static PropertyParams getProperties(String properyFile) throws IOException {
        PropertyParams propertyParams = new PropertyParams();
        Properties prop = new Properties();
        //Reading the properties from file
        InputStream fileStream = new FileInputStream(properyFile);
        prop.load(fileStream);
        propertyParams.setAvroSchemaFile(prop.getProperty("schema_file"));
        propertyParams.setSparkMasterMode(prop.getProperty("master_mode"));
        propertyParams.setSparkCheckPoint(prop.getProperty("check_point_location"));
        propertyParams.setKafkaBootServer(prop.getProperty("boot_servers"));
        propertyParams.setKafkaRawTopic(prop.getProperty("raw_kafka_topic"));
        propertyParams.setKafkaMobileTopic(prop.getProperty("mob_kafka_topic"));
        propertyParams.setDelimiter(prop.getProperty("delimiter"));
        propertyParams.setFilterName(prop.getProperty("filter_name"));
        propertyParams.setFilterValue(prop.getProperty("filter_value"));
        propertyParams.setInternalSystemInfo(prop.getProperty("internal_system_info"));
        logger.info("Set the property values");
        return propertyParams;
    }
}
