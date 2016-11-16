package com.heroku.kafka.demo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class DemoHerokuConsumer {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Please provide a topic name to consume");
      System.exit(1);
    }

    String topic = args[0];

    KafkaConfig kafkaConfig = new KafkaConfig();

    Consumer consumer = kafkaConfig.getConsumer(topic);

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(100);

      for (ConsumerRecord<String, String> record : records) {

        System.out.println(
                "Topic " +  topic +
                        " Partition " + record.partition() +
                        " Offset " + record.offset() +
                        " Value " + record.value());
      }

      if (records.count() > 0)
        break;
    }
  }
}
