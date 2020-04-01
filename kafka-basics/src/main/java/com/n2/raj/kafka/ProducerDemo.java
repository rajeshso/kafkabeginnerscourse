package com.n2.raj.kafka;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
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

    //Safe producer properties
    Properties safeProperties = new Properties(7);
    safeProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    safeProperties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    safeProperties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    safeProperties.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
    safeProperties.put(ACKS_CONFIG, "all");
    safeProperties.put(RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    safeProperties.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

    //High throughput Producer
    Properties highthroughputProperties = new Properties(10);
    highthroughputProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    highthroughputProperties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    highthroughputProperties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    highthroughputProperties.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
    highthroughputProperties.put(ACKS_CONFIG, "all");
    highthroughputProperties.put(RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    highthroughputProperties.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    highthroughputProperties.put(COMPRESSION_TYPE_CONFIG,"snappy");
    highthroughputProperties.put(LINGER_MS_CONFIG, "20");
    highthroughputProperties.put(BATCH_SIZE_CONFIG, Integer.toString(32*1024));

    //create producer
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(highthroughputProperties);
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
