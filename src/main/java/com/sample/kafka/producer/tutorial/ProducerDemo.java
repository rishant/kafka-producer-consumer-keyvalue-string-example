package com.sample.kafka.producer.tutorial;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

  public static void main(String[] args) {
    //create producer properties
    String bootStrapServers = "127.0.0.1:9091";
    
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    
    //create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
    
    //Create producer record
    ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello My first Java Message!");
    //send data
    producer.send(record);
    producer.flush();
    producer.close();
    System.out.println("Done");
  }
}
