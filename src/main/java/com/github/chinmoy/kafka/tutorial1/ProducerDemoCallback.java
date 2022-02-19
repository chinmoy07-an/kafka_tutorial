package com.github.chinmoy.kafka.tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerDemoCallback {

    public static void main(String[] args) {
        //System.out.println("Hello World");

        String bootstrapServers = "localhost:9092";

        //create a logger for my class

        Logger  logger = LoggerFactory.getLogger(ProducerDemoCallback.class);

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

        for (int i=0; i<5; i++) {

            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "So lets begin count  " + Integer.toString(i));

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
            });
        }

                //to wait for data to be produced you can do

                producer.flush(); //forces all data to be produced(flush data)

                producer.close(); //flush and close producer

        System.out.println("The demo is done ");




    }
}

