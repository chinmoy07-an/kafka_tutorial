package com.github.chinmoy.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignSeek {

    public static void main(String[] args) {

       // --testing just System.out.println("hello World");

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());


        // so in this we wont use the groupID and also dnt want to subscribe to topics (Meaning whatever we want to read from)
        String bootStrapServers = "localhost:9092";
        //String groupId = "my-fourth-application";
        String topic = "first_topic";

        //create consumer Configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest/latest/none -- 3 options


        //create consumer

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //----assign and seek API are two APIs which are used to replay data or fetch Specific message --------

        //subscribe consumer to our topic(s)

        //consumer.subscribe(Collections.singleton(topic)); // -- here coz of singleton only one topic can be provided

        //Now for if multiple topics

       // consumer.subscribe(Arrays.asList(topic));


        //********assign**********

        TopicPartition PartitionToReadFrom = new TopicPartition(topic ,0);
        long offsetToReadFrom = 10L;

        consumer.assign(Arrays.asList(PartitionToReadFrom));

        //*******seek --to get(go to) the offset-----

        consumer.seek(PartitionToReadFrom,offsetToReadFrom); // so meaning consumer will read this topic partition and also read from offset 15

        //---so now lets exit after 5 messages are read

        // poll for new Data --consumer doesn't get data until it asks for it

        int NumberofMessagesToread = 5;

        boolean keepOnReading = true;

        int  numberofMessagesReadsoFar = 0;

        while (keepOnReading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // --timeout


            // consumer reads all messages from partition 1 then partition 2 ...so on
            for (ConsumerRecord<String,String> record : records){
                numberofMessagesReadsoFar += 1;
                logger.info("key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset() );

                if (numberofMessagesReadsoFar >= NumberofMessagesToread)
                {
                    keepOnReading = false; // to exit the while loop
                    break; //to exit the for loop
                }
                


            }

        }



        System.out.println("Exiting the Application");

    }
}
