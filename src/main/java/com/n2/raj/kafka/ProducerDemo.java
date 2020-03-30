package com.n2.raj.kafka;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
//➜  cp-all-in-one git:(5.4.1-post) ✗ docker-compose exec broker kafka-console-consumer --bootstrap-server broker:9092 --topic first_topic --group myGroup3
public class ProducerDemo {

  public static void main(String[] args) {
    //create properties
    Properties properties = new Properties(3);
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    //create producer
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
    //create producer record
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic","hello world");
    //send data
    kafkaProducer.send(producerRecord);
    //flush and close the producer
    kafkaProducer.flush();
     // flush and close
    kafkaProducer.close();
  }

}
