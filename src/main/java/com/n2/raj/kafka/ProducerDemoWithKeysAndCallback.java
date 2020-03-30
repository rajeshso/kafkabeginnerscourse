package com.n2.raj.kafka;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//➜  cp-all-in-one git:(5.4.1-post) ✗ docker-compose exec broker kafka-console-consumer --bootstrap-server broker:9092 --topic second_topic --group myGroup2
//Note the keys with the same id go to the same partition
public class ProducerDemoWithKeysAndCallback {
  final static Logger LOGGER = LoggerFactory.getLogger(ProducerDemoWithKeysAndCallback.class);
  public static void main(String[] args) throws ExecutionException, InterruptedException {
    //create properties
    Properties properties = new Properties(3);
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    //create producer
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
    final String topic = "second_topic";
    final String message = "hello world ";
    for (int i=0;i<10;i++) {
      //create producer record
      String key = "key_"+i;
      LOGGER.info("\nKey is "+ key);
      ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, message +i);
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
      }).get(); //block the .send() to make it synchronous - don't do this in production
      //flush and close the producer
      kafkaProducer.flush();
    }
     // flush and close
    kafkaProducer.close();
  }

}
