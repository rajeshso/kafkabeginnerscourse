package com.n2.raj.kafka;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//Assign and Seek are used to replay data or fetch a specific message
public class ConsumerDemoWithAssignAndSeek {

  final static Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoWithAssignAndSeek.class);

  public static void main(String[] args) {
    final String topic = "second_topic";
    final String bootstrapServer = "127.0.0.1:9092";
    //Create Consumer Properties
    Properties properties = new Properties(5);
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
    properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
    //earliest is read from beginning
    //latest is read only latest messages
    //none is throw error when there is no offset stored
    //Create Kafka Consumer
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

    //Assign
    TopicPartition topicPartition = new TopicPartition(topic, 0);
    kafkaConsumer.assign(Collections.singletonList(topicPartition));
    long offsetToReadFrom = 15L;
    //Seek
    kafkaConsumer.seek(topicPartition, offsetToReadFrom);

    int numberOfMessagesToRead = 5;
    int numberOfMessagesReadSoFar = 0;
    boolean keepOnReading = true;

    //Poll for new data
    while (keepOnReading) {
      ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
      for(ConsumerRecord<String, String> record : records) {
        numberOfMessagesReadSoFar +=1;
        LOGGER.info("\nKey : "+ record.key() +" , Value : "+ record.value());
        LOGGER.info("\nPartition : "+ record.partition() + " , Offset : "+ record.offset());
        if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
          keepOnReading = false; // exit for while loop
          break; //exit for-loop
        }
      }
      LOGGER.info("Exiting the application");
    }
  }

}
