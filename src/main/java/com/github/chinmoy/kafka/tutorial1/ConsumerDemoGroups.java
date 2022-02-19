package com.github.chinmoy.kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoGroups {

    public static void main(String[] args) {

       // --testing just System.out.println("hello World");

        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());

        String bootStrapServers = "localhost:9092";

        // -- In this we will see how the consumer groups behaves balancing--------------
        //Now if you run the consumer with having my-fourth-application it will not show any messages consumed even though offset provided earliest
        //Because its was already started and there is no lag--so assigned partition -0,1,2 ( 1 instance of the group)

        //----Now lets  try with some other group my-fifth-application and restart all messages will flow in
        //---so changing application-ID you reset the application

        //-------------Rebalances--------------------
        //--clear and start the second consumer -----------if you not able to go to run edit configurations --allow multiple instances
        // now see the log you will find for one instance of CG Setting newly assigned partitions [first_topic-2]
        //And for the other Setting newly assigned partitions [first_topic-0, first_topic-1] due to rebalancing

        //Now run producerDemokeys you will partition 2 in one CG and partition 0,1 in other CG instance

        //Terminate one instance and check again the logs you will find Setting newly assigned partitions [first_topic-0, first_topic-1, first_topic-2]

        //So it rebalanced after the consumer was down thats how it works


        String groupId = "my-fifth-application";
        String topic = "first_topic";

        //create consumer Configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest/latest/none -- 3 options


        //create consumer

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer to our topic(s)

        //consumer.subscribe(Collections.singleton(topic)); // -- here coz of singleton only one topic can be provided

        //Now for if multiple topics

        consumer.subscribe(Arrays.asList(topic));

        // poll for new Data --consumer doesn't get data until it asks for it

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // --timeout


            // consumer reads all messages from partition 1 then partition 2 ...so on
            for (ConsumerRecord<String,String> record : records){

                logger.info("key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset() );
                


            }

        }




    }
}
