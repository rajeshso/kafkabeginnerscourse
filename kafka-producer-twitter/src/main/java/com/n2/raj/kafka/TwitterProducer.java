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

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterProducer {

  private static final Logger LOGGER  = LoggerFactory.getLogger(TwitterProducer.class.getName());

  public static final String CONSUMER_API_KEY = "z3ShzgU7ZMM1QGTe2RiJQquKX";
  public static final String CONSUMER_API_SECRET = "uv1xirJbMfIGxsMOU8AmYZV0VAn6o5t6Mk4JkC02BXz1xAG72M";
  public static final String ACCESS_TOKEN = "2708974422-nP7H7OwewNEXj7o6bHFiQKRhgdZroIfP8EoMtBP";
  public static final String ACCESS_TOKEN_SECRET = "A9Rt67LM6wFyZqvuFW4E7li9L8mjaDGheB0nTu6lS4J3w";
  List<String> terms = Lists.newArrayList("kafka","CCDAK", "bitcoin");

  public TwitterProducer() {

  }

  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  public void run() {
    LOGGER.info("Setup");
    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
    //create a twitter client
    final Client client = twitterClient(msgQueue);
    client.connect();
    //create a kafka producer
    KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

    //add a shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOGGER.info("Stopping application....");
      LOGGER.info("Shutting down client from twitter...");
      client.stop();
      LOGGER.info("Closing producer...");
      kafkaProducer.close();
      LOGGER.info("Done!!");
    }));
    //loop to send tweets to kafka
    // on a different thread, or multiple different threads....
    while (!client.isDone()) {
      String msg = null;
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        client.stop();
      }
      if (msg!=null) {
        LOGGER.info(msg);
        kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
          @Override
          public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e!=null) {
              LOGGER.error("Some error happened", e);
            }
          }
        });
      }
    }
    LOGGER.info("End of Application");
  }

  public Client twitterClient(BlockingQueue<String> msgQueue) {

    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    // Optional: set up some followings and track terms
    //List<Long> followings = Lists.newArrayList(1234L, 566788L);//people
    //hosebirdEndpoint.followings(followings);
    hosebirdEndpoint.trackTerms(terms);

    // These secrets should be read from a config file
    Authentication hosebirdAuth = new OAuth1(CONSUMER_API_KEY,
        CONSUMER_API_SECRET,
        ACCESS_TOKEN,
        ACCESS_TOKEN_SECRET);

    ClientBuilder builder = new ClientBuilder()
        .name("Hosebird-Client-01")                              // optional: mainly for the logs
        .hosts(hosebirdHosts)
        .authentication(hosebirdAuth)
        .endpoint(hosebirdEndpoint)
        .processor(new StringDelimitedProcessor(
            msgQueue));                   // optional: use this if you want to process client events

    Client hosebirdClient = builder.build();
    return hosebirdClient;
  }

  private KafkaProducer<String, String> createKafkaProducer() {
    //create properties
    Properties properties = new Properties(3);
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    //create producer
    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(getHighThroughputProperties());
    return kafkaProducer;
  }

  private Properties getHighThroughputProperties() {
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
    return highthroughputProperties;
  }
}
