package com.github.chinmoy.kafka.tutorial1;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //System.out.println("Hello World");

        String bootstrapServers = "localhost:9092";

        //create a logger for my class

        Logger  logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        Properties properties = new Properties();

        //1st Stage create Producer properties old way
        //properties.setProperty("bootstrap.servers",bootstrapServers);
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        //properties.setProperty("value.serializer",StringSerializer.class.getName());

        //create Producer properties new way using Producer Config
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //create producer record

        //let's try to do a bunch of time

        //-----This time we will be using the Keys



        for (int i=0; i<5; i++) {

            String topic = "first_topic";
            String value = "duplicate" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);

            logger.info("Key: " + key); // log the key

            //---first run -----------
            //id_0 = partition 1
            //id_1 = 0
            //id_2  = 2
            //id_3 = 0
            //id_4 = 2

            //----second run -----------the same as before so it means the same key goes into the same partition


            //send data--it takes producer record as input

            //producer.send(record); // This is asynchronous so it happens in background
            //so once the above is executed basically the program exits and data is never sent

            //----Callback------

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime a record is successfully sent or exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp " + recordMetadata.timestamp());
                    } else {

                        logger.error("error while producing : " + e);
                    }
                }
            }).get(); //--Force(block) the send to be synchronous --Please do not try this in production
        }

                //to wait for data to be produced you can do

                producer.flush(); //forces all data to be produced(flush data)

                producer.close(); //flush and close producer

        System.out.println("The demo is done ");




    }
}

