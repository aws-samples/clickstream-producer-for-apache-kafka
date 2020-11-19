package com.amazonaws.kafka.samples;

import com.amazonaws.kafka.samples.saslscram.Secrets;
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import samples.clickstream.avro.ClickEvent;
import software.amazon.awssdk.services.glue.model.Compatibility;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
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
    private final Boolean sslEnable;
    private final Boolean mTLSEnable;
    private final Boolean saslScramEnable;
    private final Boolean glueSchemaRegistry;
    private static final Logger logger = LogManager.getLogger(KafkaClickstreamClient.class);

    private static String getSaslScramString() {
        String secretNamePrefix = "AmazonMSK_";
        String secret = Secrets.getSecret(secretNamePrefix + KafkaClickstreamClient.saslscramUser, Secrets.getSecretsManagerClient(KafkaClickstreamClient.region));
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode;
        try {
            jsonNode = objectMapper.readTree(secret);
        } catch (IOException e) {
            logger.error("Error reading returned secret for user {} \n", KafkaClickstreamClient.saslscramUser);
            logger.error(Util.stackTrace(e));
            throw new RuntimeException(String.format("Error reading returned secret for user %s \n", KafkaClickstreamClient.saslscramUser));
        }
        String password = jsonNode.get("password").asText();
        return "org.apache.kafka.common.security.scram.ScramLoginModule required username=" + KafkaClickstreamClient.saslscramUser + " password=" + password + ";";
    }

    KafkaProducerFactory(String propertiesFilePath, Boolean sslEnable, Boolean mTLSEnable, Boolean saslScramEnable, Boolean glueSchemaRegistry) {
        this.propertiesFilePath = propertiesFilePath;
        if (mTLSEnable) {
            this.sslEnable = true;
        } else {
            this.sslEnable = sslEnable;
        }
        this.mTLSEnable = mTLSEnable;
        this.saslScramEnable = saslScramEnable;
        this.glueSchemaRegistry = glueSchemaRegistry;
    }


    Producer<String, ClickEvent> createProducer() throws Exception {

        Properties producerProps = new Properties();
        Properties loadProps = new Properties();

        try (FileInputStream file = new FileInputStream(propertiesFilePath)) {
            loadProps.load(file);
            producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, loadProps.getProperty("BOOTSTRAP_SERVERS_CONFIG", BOOTSTRAP_SERVERS_CONFIG).equals("") ? BOOTSTRAP_SERVERS_CONFIG : loadProps.getProperty("BOOTSTRAP_SERVERS_CONFIG", BOOTSTRAP_SERVERS_CONFIG));
            producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, loadProps.getProperty("CLIENT_ID_CONFIG", CLIENT_ID_CONFIG).equals("") ? CLIENT_ID_CONFIG : loadProps.getProperty("CLIENT_ID_CONFIG", CLIENT_ID_CONFIG));

            if (sslEnable) {
                producerProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SECURITY_PROTOCOL_CONFIG);
                producerProps.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, loadProps.getProperty("SSL_TRUSTSTORE_LOCATION_CONFIG", SSL_TRUSTSTORE_LOCATION_CONFIG).equals("") ? SSL_TRUSTSTORE_LOCATION_CONFIG : loadProps.getProperty("SSL_TRUSTSTORE_LOCATION_CONFIG", SSL_TRUSTSTORE_LOCATION_CONFIG));
            }
            if (mTLSEnable) {
                producerProps.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, loadProps.getProperty("SSL_KEYSTORE_LOCATION_CONFIG", SSL_KEYSTORE_LOCATION_CONFIG).equals("") ? SSL_KEYSTORE_LOCATION_CONFIG : loadProps.getProperty("SSL_KEYSTORE_LOCATION_CONFIG", SSL_KEYSTORE_LOCATION_CONFIG));
                producerProps.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, loadProps.getProperty("SSL_KEYSTORE_PASSWORD_CONFIG", SSL_KEYSTORE_PASSWORD_CONFIG).equals("") ? SSL_KEYSTORE_PASSWORD_CONFIG : loadProps.getProperty("SSL_KEYSTORE_PASSWORD_CONFIG", SSL_KEYSTORE_PASSWORD_CONFIG));
                producerProps.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, loadProps.getProperty("SSL_KEY_PASSWORD_CONFIG", SSL_KEY_PASSWORD_CONFIG).equals("") ? SSL_KEY_PASSWORD_CONFIG : loadProps.getProperty("SSL_KEY_PASSWORD_CONFIG", SSL_KEY_PASSWORD_CONFIG));
            }
            if (saslScramEnable) {
                producerProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
                producerProps.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
                producerProps.setProperty(SaslConfigs.SASL_JAAS_CONFIG, getSaslScramString());
            }

            if (glueSchemaRegistry){
                producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AWSKafkaAvroSerializer.class.getName());
                producerProps.setProperty(AWSSchemaRegistryConstants.AWS_REGION, KafkaClickstreamClient.gsrRegion);
                producerProps.setProperty(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());
                if (KafkaClickstreamClient.gsrAutoRegistration)
                    producerProps.setProperty(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, "true");
                if (KafkaClickstreamClient.gsrRegistryName != null)
                    producerProps.setProperty(AWSSchemaRegistryConstants.REGISTRY_NAME, KafkaClickstreamClient.gsrRegistryName);
                if (KafkaClickstreamClient.gsrSchemaName != null)
                    producerProps.setProperty(AWSSchemaRegistryConstants.SCHEMA_NAME, KafkaClickstreamClient.gsrSchemaName);
                if (KafkaClickstreamClient.gsrSchemaDescription != null)
                    producerProps.setProperty(AWSSchemaRegistryConstants.DESCRIPTION, KafkaClickstreamClient.gsrSchemaDescription);
                if (KafkaClickstreamClient.gsrCompatibilitySetting != null)
                    producerProps.setProperty(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, Compatibility.fromValue(KafkaClickstreamClient.gsrCompatibilitySetting).toString());
            } else {
                producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, loadProps.getProperty("VALUE_SERIALIZER_CLASS_CONFIG", VALUE_SERIALIZER_CLASS_CONFIG).equals("") ? VALUE_SERIALIZER_CLASS_CONFIG : loadProps.getProperty("VALUE_SERIALIZER_CLASS_CONFIG", VALUE_SERIALIZER_CLASS_CONFIG));
                producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, loadProps.getProperty("KEY_SERIALIZER_CLASS_CONFIG", KEY_SERIALIZER_CLASS_CONFIG).equals("") ? KEY_SERIALIZER_CLASS_CONFIG : loadProps.getProperty("KEY_SERIALIZER_CLASS_CONFIG", KEY_SERIALIZER_CLASS_CONFIG));
                producerProps.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, loadProps.getProperty("SCHEMA_REGISTRY_URL_CONFIG", SCHEMA_REGISTRY_URL_CONFIG).equals("") ? SCHEMA_REGISTRY_URL_CONFIG : loadProps.getProperty("SCHEMA_REGISTRY_URL_CONFIG", SCHEMA_REGISTRY_URL_CONFIG));
            }

            producerProps.setProperty(ProducerConfig.ACKS_CONFIG, loadProps.getProperty("ACKS_CONFIG", ACKS_CONFIG).equals("") ? ACKS_CONFIG : loadProps.getProperty("ACKS_CONFIG", ACKS_CONFIG));
            producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, loadProps.getProperty("RETRIES_CONFIG", RETRIES_CONFIG).equals("") ? RETRIES_CONFIG : loadProps.getProperty("RETRIES_CONFIG", RETRIES_CONFIG));

        } catch (FileNotFoundException e) {
            logger.info("Properties file not found in location: {}, using defaults \n", this.propertiesFilePath);
            producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
            producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID_CONFIG);

            if (sslEnable) {
                producerProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SECURITY_PROTOCOL_CONFIG);
                producerProps.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, SSL_TRUSTSTORE_LOCATION_CONFIG);
            }
            if (mTLSEnable) {
                producerProps.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, SSL_KEYSTORE_LOCATION_CONFIG);
                producerProps.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, SSL_KEYSTORE_PASSWORD_CONFIG);
                producerProps.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, SSL_KEY_PASSWORD_CONFIG);
            }
            if (saslScramEnable) {
                producerProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
                producerProps.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
                producerProps.setProperty(SaslConfigs.SASL_JAAS_CONFIG, getSaslScramString());
            }

            if (glueSchemaRegistry){
                producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AWSKafkaAvroSerializer.class.getName());
                producerProps.setProperty(AWSSchemaRegistryConstants.AWS_REGION, KafkaClickstreamClient.gsrRegion);
                producerProps.setProperty(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());
                if (KafkaClickstreamClient.gsrAutoRegistration)
                    producerProps.setProperty(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, "true");
                if (KafkaClickstreamClient.gsrRegistryName != null)
                    producerProps.setProperty(AWSSchemaRegistryConstants.REGISTRY_NAME, KafkaClickstreamClient.gsrRegistryName);
                if (KafkaClickstreamClient.gsrSchemaName != null)
                    producerProps.setProperty(AWSSchemaRegistryConstants.SCHEMA_NAME, KafkaClickstreamClient.gsrSchemaName);
                if (KafkaClickstreamClient.gsrSchemaDescription != null)
                    producerProps.setProperty(AWSSchemaRegistryConstants.DESCRIPTION, KafkaClickstreamClient.gsrSchemaDescription);
                if (KafkaClickstreamClient.gsrCompatibilitySetting != null)
                    producerProps.setProperty(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, Compatibility.fromValue(KafkaClickstreamClient.gsrCompatibilitySetting).toString());
            } else {
                producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, loadProps.getProperty("VALUE_SERIALIZER_CLASS_CONFIG", VALUE_SERIALIZER_CLASS_CONFIG).equals("") ? VALUE_SERIALIZER_CLASS_CONFIG : loadProps.getProperty("VALUE_SERIALIZER_CLASS_CONFIG", VALUE_SERIALIZER_CLASS_CONFIG));
                producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, loadProps.getProperty("KEY_SERIALIZER_CLASS_CONFIG", KEY_SERIALIZER_CLASS_CONFIG).equals("") ? KEY_SERIALIZER_CLASS_CONFIG : loadProps.getProperty("KEY_SERIALIZER_CLASS_CONFIG", KEY_SERIALIZER_CLASS_CONFIG));
                producerProps.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, loadProps.getProperty("SCHEMA_REGISTRY_URL_CONFIG", SCHEMA_REGISTRY_URL_CONFIG).equals("") ? SCHEMA_REGISTRY_URL_CONFIG : loadProps.getProperty("SCHEMA_REGISTRY_URL_CONFIG", SCHEMA_REGISTRY_URL_CONFIG));
            }

            producerProps.setProperty(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG);
            producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, RETRIES_CONFIG);
        }

        return new KafkaProducer<>(producerProps);
    }
}
