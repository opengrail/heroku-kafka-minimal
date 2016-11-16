package com.opengrail.heroku.kafka.demo;

import com.github.jkutner.EnvKeyStore;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static java.lang.String.format;
import static java.lang.System.err;
import static java.lang.System.exit;
import static java.lang.System.getenv;
import static java.util.Collections.singletonList;

public class KafkaConfig {

  private final Properties properties;

  KafkaConfig() {

    String brokers = checkBrokerEnv();

    properties = new Properties();
    List<String> hostPorts = Lists.newArrayList();

    for (String url : Splitter.on(",").split(brokers)) {
      try {
        URI uri = new URI(url);
        hostPorts.add(format("%s:%d", uri.getHost(), uri.getPort()));

        switch (uri.getScheme()) {
          case "kafka":
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
            break;
          case "kafka+ssl":
            if (null == properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)) {
              properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");

              try {
                EnvKeyStore envTrustStore = EnvKeyStore.createWithRandomPassword("KAFKA_TRUSTED_CERT");
                EnvKeyStore envKeyStore = EnvKeyStore.createWithRandomPassword("KAFKA_CLIENT_CERT_KEY", "KAFKA_CLIENT_CERT");

                File trustStore = envTrustStore.storeTemp();
                File keyStore = envKeyStore.storeTemp();

                properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, envTrustStore.type());
                properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStore.getAbsolutePath());
                properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, envTrustStore.password());
                properties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, envKeyStore.type());
                properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStore.getAbsolutePath());
                properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, envKeyStore.password());
              } catch (Exception e) {
                throw new RuntimeException("There was a problem creating the Kafka key stores", e);
              }
            }
            break;
          default:
            throw new IllegalArgumentException(format("unknown scheme; %s", uri.getScheme()));
        }
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, Joiner.on(",").join(hostPorts));
  }

  final Consumer getConsumer(String topic) {

    Properties config = (Properties) properties.clone();

    config.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-unshared-group-demo-" + new Random().nextInt());
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    Consumer consumer = new KafkaConsumer<>(config);
    consumer.subscribe(singletonList(topic));

    return consumer;
  }

  private static String checkBrokerEnv() {

    String brokers = getenv("KAFKA_URL");

    if (brokers == null) {
      err.println("You need to set the KAFKA_URL environment variables");
      exit(1);
    }

    if (brokers.startsWith("kafka+ssl")) {

      String kafka_trusted_cert = getenv("KAFKA_TRUSTED_CERT");
      String kafka_client_cert_key = getenv("KAFKA_CLIENT_CERT_KEY");
      String kafka_client_cert = getenv("KAFKA_CLIENT_CERT");
      if (kafka_trusted_cert == null || kafka_client_cert_key == null || kafka_client_cert == null) {
        err.println("You need to set the SSL environment variables");
        exit(1);
      }
    }

    return brokers;

  }

}