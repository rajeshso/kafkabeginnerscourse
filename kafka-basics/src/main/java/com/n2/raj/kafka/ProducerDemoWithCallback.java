package com.n2.raj.kafka;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//➜  cp-all-in-one git:(5.4.1-post) ✗ docker-compose exec broker kafka-console-consumer --bootstrap-server broker:9092 --topic second_topic --group myGroup2
public class ProducerDemoWithCallback {
  final static Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
  public static void main(String[] args) {
    //create properties
    Properties properties = new Properties(3);
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    //create producer
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
    for (int i=0;i<10;i++) {
      //create producer record
      ProducerRecord<String, String> producerRecord = new ProducerRecord<>("second_topic","hello world "+i);
      //send data
      kafkaProducer.send(producerRecord, new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          if (e==null) {
            LOGGER.info("\nReceived new metadata");
            LOGGER.info("\nTopic: "+ recordMetadata.topic());
            LOGGER.info("\nPartition: "+recordMetadata.partition());
            LOGGER.info("\nOffset: "+recordMetadata.offset());
            LOGGER.info("\nTimestamp: "+recordMetadata.timestamp());
          }else {
            System.err.println(e.getMessage());
          }
        }
      });
      //flush and close the producer
      kafkaProducer.flush();
    }
     // flush and close
    kafkaProducer.close();
  }

}
