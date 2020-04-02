package com.n2.raj.kafka;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import com.google.gson.JsonParser;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
  private static final String TWITTER_TWEETS_TOPIC = "twitter_tweets";

  public static RestHighLevelClient createClient(){

    //////////////////////////
    /////////// IF YOU USE LOCAL ELASTICSEARCH
    //////////////////////////

    //  String hostname = "localhost";
    //  RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));


    //////////////////////////
    /////////// IF YOU USE BONSAI / HOSTED ELASTICSEARCH
    //////////////////////////
    //https://
    // replace with your own credentials
    String hostname = "kafka-course-ABCD.eu-west-1.bonsaisearch.net"; // localhost or bonsai url
    String username = "ABCD"; // needed only for bonsai
    String password = "ABCD"; // needed only for bonsai

    // credentials provider help supply username and password
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(username, password));

    RestClientBuilder builder = RestClient.builder(
        new HttpHost(hostname, 443, "https"))
        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
          @Override
          public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
          }
        });

    RestHighLevelClient client = new RestHighLevelClient(builder);
    return client;
  }

  public static KafkaConsumer<String, String> createConsumer(String topic) {
    final String bootstrapServer = "127.0.0.1:9092";
    final String groupId = "kafka-demo-elasticsearch";
    //Create Consumer Properties
    Properties properties = new Properties(5);
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
    properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(GROUP_ID_CONFIG, groupId);
    properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.setProperty(MAX_POLL_RECORDS_CONFIG,"100");
    //earliest is read from beginning
    //latest is read only latest messages
    //none is throw error when there is no offset stored
    //Create Kafka Consumer
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
    //Subscribe Consumer to our topic(s)
    kafkaConsumer.subscribe(Collections.singleton(topic));
    return kafkaConsumer;
  }
  public static void main(String[] args) throws IOException {
    RestHighLevelClient client = createClient();
    KafkaConsumer<String, String> kafkaConsumer = createConsumer(TWITTER_TWEETS_TOPIC);
    //Poll for new data
    while (true) {
      ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
      int recordCount = records.count();
      LOGGER.info("Received "+ recordCount + " records ");
      BulkRequest bulkRequest = new BulkRequest();
      for(ConsumerRecord<String, String> record : records) {
        String jsonString = record.value();
        // 2 strategies
        // kafka generic ID
        // String id = record.topic() + "_" + record.partition() + "_" + record.offset();
        try {
          String id = extractIdFromTweet(record.value());
          IndexRequest indexRequest = new IndexRequest(
              "twitter",
              "tweets"
          ).source(jsonString, XContentType.JSON)
              .id(id);
          bulkRequest.add(indexRequest);
        } catch (NullPointerException e) {
          LOGGER.warn("Skipping bad data: "+ record.value());
          e.printStackTrace();
        }
        //IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
      //  String id = indexResponse.getId();
       // LOGGER.info("id is "+indexResponse.getId());
/*        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }*/
      }
      if (recordCount> 0) {
        BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        LOGGER.info("Committing offsets...");
        kafkaConsumer.commitSync();
        LOGGER.info("Offsets have been committed");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    //client.close();
  }
  private static JsonParser jsonParser = new JsonParser();

  private static String extractIdFromTweet(String tweetJson){
    // gson library
    return jsonParser.parse(tweetJson)
        .getAsJsonObject()
        .get("id_str")
        .getAsString();
  }
}
