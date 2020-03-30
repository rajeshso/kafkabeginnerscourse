package com.n2.raj.kafka;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//docker-compose exec broker kafka-consumer-groups --bootstrap-server localhost:9092 --group myGroup4 --describe
//docker-compose exec broker kafka-consumer-groups --bootstrap-server localhost:9092 --group myGroup4 --topic second_topic --reset-offsets --shift-by -2 --execute
public class ConsumerDemoWithConsumerGroups {

  final static Logger LOGGER = LoggerFactory.getLogger(ConsumerDemoWithConsumerGroups.class);

  public static void main(String[] args) {
    final String topic = "second_topic";
    final String bootstrapServer = "127.0.0.1:9092";
    final String groupId = "myGroup4";
    //Create Consumer Properties
    Properties properties = new Properties(5);
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
    properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(GROUP_ID_CONFIG, groupId);
    properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
    //earliest is read from beginning
    //latest is read only latest messages
    //none is throw error when there is no offset stored
    //Create Kafka Consumer
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
    //Subscribe Consumer to our topic(s)
    kafkaConsumer.subscribe(Collections.singleton(topic));
    //Poll for new data
    while (true) {
      ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
      for(ConsumerRecord<String, String> record : records) {
        LOGGER.info("\nKey : "+ record.key() +" , Value : "+ record.value());
        LOGGER.info("\nPartition : "+ record.partition() + " , Offset : "+ record.offset());
      }
    }
  }

}
