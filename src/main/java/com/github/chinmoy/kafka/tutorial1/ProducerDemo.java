package com.github.chinmoy.kafka.tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

    public static void main(String[] args) {
        //System.out.println("Hello World");

        String bootstrapServers = "localhost:9092";

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

        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic","Hello World wassup");

        //send data--it takes producer record as input

        producer.send(record); // This is asynchronous so it happens in background
        //so once the above is executed basically the program exits and data is never sent

        //to wait for data to be produced you can do

        producer.flush(); //forces all data to be produced(flush data)

        producer.close(); //flush and close producer

        System.out.println("Hello World");




    }
}
