package com.kindredgroup;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Created by rahumathulla on 12/16/2019.
 */
public class CommonUtils {
    static Logger logger = Logger.getLogger(CommonUtils.class);
    // Returns a random string from the list
    public static String getRandomString(String[] contentList)
    {
        Random rand = new Random();
        int randomNumber=rand.nextInt(contentList.length);
        return contentList[randomNumber];
    }

    // Returns a random number from the list
    public static long getRandomLong(long[] contentList)
    {
        Random rand = new Random();
        int randomNumber=rand.nextInt(contentList.length);
        return contentList[randomNumber];
    }

    /**
     *
     * @returns a random number
     */
    public static long genCustomerID(){
        long numericUUID = new Random().nextLong();
        return numericUUID;

    }


    /**
     *
     * @returns avro schema based on avro structure mentioned in avcFilePath
     */
    public static String getAvroSchema(String avcFilePath) throws IOException {

        StringBuilder avroSchema = new StringBuilder();
        Stream<String> stream = Files.lines( Paths.get(avcFilePath), StandardCharsets.UTF_8);
        stream.forEach(s -> avroSchema.append(s).append("\n"));
        logger.info("Set Avro Schema");
        return avroSchema.toString();
    }
}

