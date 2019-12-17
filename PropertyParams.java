package com.kindredgroup;

/**
 * Created by rahumathulla on 12/16/2019.
 */
public class PropertyParams {
    private String avroSchemaFile;
    private String sparkMasterMode;
    private String sparkCheckPoint;
    private String kafkaBootServer;
    private String kafkaRawTopic;
    private String kafkaMobileTopic;
    private String delimiter;
    private String filterValue;
    private String internalSystemInfo;
    private String filterName;

    public String getAvroSchemaFile() {
        return avroSchemaFile;
    }

    public void setAvroSchemaFile(String avroSchemaFile) {
        this.avroSchemaFile = avroSchemaFile;
    }

    public String getSparkMasterMode() {
        return sparkMasterMode;
    }

    public void setSparkMasterMode(String sparkMasterMode) {
        this.sparkMasterMode = sparkMasterMode;
    }

    public String getSparkCheckPoint() {
        return sparkCheckPoint;
    }

    public void setSparkCheckPoint(String sparkCheckPoint) {
        this.sparkCheckPoint = sparkCheckPoint;
    }

    public String getKafkaBootServer() {
        return kafkaBootServer;
    }

    public void setKafkaBootServer(String kafkaBootServer) {
        this.kafkaBootServer = kafkaBootServer;
    }

    public String getKafkaRawTopic() {
        return kafkaRawTopic;
    }

    public void setKafkaRawTopic(String kafkaRawTopic) {
        this.kafkaRawTopic = kafkaRawTopic;
    }

    public String getKafkaMobileTopic() {
        return kafkaMobileTopic;
    }

    public void setKafkaMobileTopic(String kafkaMobileTopic) {
        this.kafkaMobileTopic = kafkaMobileTopic;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getFilterValue() {
        return filterValue;
    }

    public void setFilterValue(String filterValue) {
        this.filterValue = filterValue;
    }

    public String getFilterName() {
        return filterName;
    }

    public void setFilterName(String filterName) {
        this.filterName = filterName;
    }

    public String getInternalSystemInfo() {
        return internalSystemInfo;
    }

    public void setInternalSystemInfo(String internalSystemInfo) {
        this.internalSystemInfo = internalSystemInfo;
    }
}
