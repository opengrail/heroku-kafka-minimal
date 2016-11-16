package com.opengrail.heroku.kafka.demo;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class DemoApplication {

  public static void main(String[] args) throws Exception {

    final String topic = System.getenv("TOPIC");

    if (topic == null) {
      System.err.println("You need to set the TOPIC environment variable");
      System.exit(1);
    }

    final KafkaConfig kafkaConfig = new KafkaConfig();

    final HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);
    server.createContext("/kafka", new KafkaHandler(kafkaConfig, topic));
    server.setExecutor(null); // creates a default executor
    server.start();

    System.out.println("Started server . . .");

  }

  static final class KafkaHandler implements HttpHandler {
    private final String topic;
    private final KafkaConfig kafkaConfig;

    KafkaHandler(final KafkaConfig kafkaConfig, final String topic) {
      this.topic = topic;
      this.kafkaConfig = kafkaConfig;
    }

    private String getMessage() {
      Consumer consumer = kafkaConfig.getConsumer(topic);

      while (true) {

        final ConsumerRecords<String, String> records = consumer.poll(100);

        if (records.count() > 0) {

          String message = "";

          for (final ConsumerRecord<String, String> record : records) {

            message += "Topic " + topic +
                    " Partition " + record.partition() +
                    " Offset " + record.offset() +
                    " Value " + record.value();
          }

          return message;
        }

      }
    }

    @Override
    public final void handle(HttpExchange t) throws IOException {
      String response = getMessage();
      t.sendResponseHeaders(200, response.length());
      OutputStream os = t.getResponseBody();
      os.write(response.getBytes());
      os.close();
    }
  }
}