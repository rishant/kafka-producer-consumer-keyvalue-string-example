package com.sample.kafka.producer.tutorial;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {

  public static void main(String[] args) {

    Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
    // create producer properties
    String bootStrapServers = "127.0.0.1:9091";

    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    // create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    for (int i = 0; i < 10; i++) {

      // Create producer record
      ProducerRecord<String, String> record =
          new ProducerRecord<String, String>("first_topic", "Hello My first Java Message!" + i);
      // send data
      producer.send(record, new Callback() {

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
          if (exception == null) {
            logger.info(
                "Received new metadata. \nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",
                metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
          } else {
            logger.error("Error while producing", exception);
          }
        }
      });
    }
    producer.flush();
    producer.close();
    System.out.println("Done");
  }
}
