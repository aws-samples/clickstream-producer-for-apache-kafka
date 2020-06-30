package com.amazonaws.kafka.samples;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import samples.clickstream.avro.ClickEvent;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Properties;

class KafkaProducerFactory {

    private final String BOOTSTRAP_SERVERS_CONFIG = "http://127.0.0.1:9092";
    private final String SCHEMA_REGISTRY_URL_CONFIG = "http://127.0.0.1:8081";
    private final String ACKS_CONFIG = "all";
    private final String RETRIES_CONFIG = "5";
    private final String VALUE_SERIALIZER_CLASS_CONFIG = "io.confluent.kafka.serializers.KafkaAvroSerializer";
    private final String KEY_SERIALIZER_CLASS_CONFIG = "io.confluent.kafka.serializers.KafkaAvroSerializer";
    private final String SSL_TRUSTSTORE_LOCATION_CONFIG = "/tmp/kafka.client.truststore.jks";
    private final String SSL_KEYSTORE_LOCATION_CONFIG = "/tmp/kafka.client.keystore.jks";
    private final String SECURITY_PROTOCOL_CONFIG = "SSL";
    private final String SSL_KEYSTORE_PASSWORD_CONFIG = "password";
    private final String SSL_KEY_PASSWORD_CONFIG = "password";
    private final String CLIENT_ID_CONFIG = "clickstream-producer";
    private final String propertiesFilePath;
    private final boolean sslEnable;
    private final boolean mTLSEnable;
    private static final Logger logger = LogManager.getLogger(KafkaClickstreamClient.class);

    KafkaProducerFactory(String propertiesFilePath, Boolean sslEnable, Boolean mTLSEnable){
        this.propertiesFilePath = propertiesFilePath;
        if (mTLSEnable){
            this.sslEnable = true;
        } else {
            this.sslEnable = sslEnable;
        }
        this.mTLSEnable = mTLSEnable;
    }


    Producer<String, ClickEvent> createProducer() throws Exception {

        Properties producerProps = new Properties();
        Properties loadProps = new Properties();

        try (FileInputStream file = new FileInputStream(propertiesFilePath)) {
            loadProps.load(file);
            producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, loadProps.getProperty("BOOTSTRAP_SERVERS_CONFIG", BOOTSTRAP_SERVERS_CONFIG).equals("") ? BOOTSTRAP_SERVERS_CONFIG : loadProps.getProperty("BOOTSTRAP_SERVERS_CONFIG", BOOTSTRAP_SERVERS_CONFIG));
            producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, loadProps.getProperty("CLIENT_ID_CONFIG", CLIENT_ID_CONFIG).equals("") ? CLIENT_ID_CONFIG : loadProps.getProperty("CLIENT_ID_CONFIG", CLIENT_ID_CONFIG));

            //configure the following three settings for SSL Encryption
            if (sslEnable){
                producerProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SECURITY_PROTOCOL_CONFIG);
                producerProps.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, loadProps.getProperty("SSL_TRUSTSTORE_LOCATION_CONFIG", SSL_TRUSTSTORE_LOCATION_CONFIG).equals("") ? SSL_TRUSTSTORE_LOCATION_CONFIG : loadProps.getProperty("SSL_TRUSTSTORE_LOCATION_CONFIG", SSL_TRUSTSTORE_LOCATION_CONFIG));
            }
            if (mTLSEnable){
                producerProps.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, loadProps.getProperty("SSL_KEYSTORE_LOCATION_CONFIG", SSL_KEYSTORE_LOCATION_CONFIG).equals("") ? SSL_KEYSTORE_LOCATION_CONFIG : loadProps.getProperty("SSL_KEYSTORE_LOCATION_CONFIG", SSL_KEYSTORE_LOCATION_CONFIG));
                producerProps.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, loadProps.getProperty("SSL_KEYSTORE_PASSWORD_CONFIG", SSL_KEYSTORE_PASSWORD_CONFIG).equals("") ? SSL_KEYSTORE_PASSWORD_CONFIG : loadProps.getProperty("SSL_KEYSTORE_PASSWORD_CONFIG", SSL_KEYSTORE_PASSWORD_CONFIG));
                producerProps.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, loadProps.getProperty("SSL_KEY_PASSWORD_CONFIG", SSL_KEY_PASSWORD_CONFIG).equals("") ? SSL_KEY_PASSWORD_CONFIG : loadProps.getProperty("SSL_KEY_PASSWORD_CONFIG", SSL_KEY_PASSWORD_CONFIG));
            }

            producerProps.setProperty(ProducerConfig.ACKS_CONFIG, loadProps.getProperty("ACKS_CONFIG", ACKS_CONFIG).equals("") ? ACKS_CONFIG : loadProps.getProperty("ACKS_CONFIG", ACKS_CONFIG));
            producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, loadProps.getProperty("RETRIES_CONFIG", RETRIES_CONFIG).equals("") ? RETRIES_CONFIG : loadProps.getProperty("RETRIES_CONFIG", RETRIES_CONFIG));
            producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, loadProps.getProperty("VALUE_SERIALIZER_CLASS_CONFIG", VALUE_SERIALIZER_CLASS_CONFIG).equals("") ? VALUE_SERIALIZER_CLASS_CONFIG : loadProps.getProperty("VALUE_SERIALIZER_CLASS_CONFIG", VALUE_SERIALIZER_CLASS_CONFIG));
            producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, loadProps.getProperty("KEY_SERIALIZER_CLASS_CONFIG", KEY_SERIALIZER_CLASS_CONFIG).equals("") ? KEY_SERIALIZER_CLASS_CONFIG : loadProps.getProperty("KEY_SERIALIZER_CLASS_CONFIG", KEY_SERIALIZER_CLASS_CONFIG));
            producerProps.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, loadProps.getProperty("SCHEMA_REGISTRY_URL_CONFIG", SCHEMA_REGISTRY_URL_CONFIG).equals("") ? SCHEMA_REGISTRY_URL_CONFIG : loadProps.getProperty("SCHEMA_REGISTRY_URL_CONFIG", SCHEMA_REGISTRY_URL_CONFIG));

        } catch (FileNotFoundException e) {
            logger.info("Properties file not found in location: {}, using defaults \n", this.propertiesFilePath);
            producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
            producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID_CONFIG);

            if (sslEnable){
                producerProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SECURITY_PROTOCOL_CONFIG);
                producerProps.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, SSL_TRUSTSTORE_LOCATION_CONFIG);
            }
            if (mTLSEnable){
                producerProps.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, SSL_KEYSTORE_LOCATION_CONFIG);
                producerProps.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, SSL_KEYSTORE_PASSWORD_CONFIG);
                producerProps.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, SSL_KEY_PASSWORD_CONFIG);
            }

            producerProps.setProperty(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG);
            producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, RETRIES_CONFIG);
            producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS_CONFIG);
            producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS_CONFIG);
            producerProps.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL_CONFIG);
        }

        return new KafkaProducer<>(producerProps);
    }
}
