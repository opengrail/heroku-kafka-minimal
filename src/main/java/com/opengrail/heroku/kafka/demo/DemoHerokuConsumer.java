package com.opengrail.heroku.kafka.demo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class DemoHerokuConsumer {

  public static void main(String[] args) throws Exception {

    final String topic = System.getenv("TOPIC");

    if (topic == null) {
      System.err.println("You need to set the TOPIC environment variable");
      System.exit(1);
    }

    final KafkaConfig kafkaConfig = new KafkaConfig();

    final Consumer consumer = kafkaConfig.getConsumer(topic);

    System.out.println("Waiting for a message on topic: " + topic);

    while (true) {

      final ConsumerRecords<String, String> records = consumer.poll(100);

      for (final ConsumerRecord<String, String> record : records) {

        System.out.println(
                "Topic " +  topic +
                        " Partition " + record.partition() +
                        " Offset " + record.offset() +
                        " Value " + record.value());
      }
      
      if (records.count() > 0)
        break;
    }

    System.out.println("Connectivity demo complete");

  }
}
