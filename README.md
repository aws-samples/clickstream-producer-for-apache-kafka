This is a Kafka Producer that generates mock Clickstream data for an imaginary e-commerce site. The scenario is that 
a user logs into the site, lands on the home page, browses the product catalog, lands on individual product pages, and 
either adds the product to cart or not and conitues to do that until the user either creates an order and does order 
checkout or abandons the session.
   
The producer generates the events in a user session in sequence but runs multiple threads to simulate multiple users 
hitting the site. The number of threads can be specified by using parameters. It utilizes a Schema Registry and 
generates Avro encoded events. It supports both the AWS Glue Schema Registry and a 3rd party Schema Registry.

For the AWS Glue Schema Registry, the producer accepts parameters to use a specific registry, pre-created schema name, 
schema description, compatibility mode (to check compatibility for schema evolution) and whether to turn on 
auto registration of schemas. If those parameters are not specified but using the AWS Glue Schema registry is specified, 
it uses the default schema registry. Some of the parameters may need to be specified if others are not. 
The AWS Glue Schema Registry provides open sourced serde libraries for serialization and deserialization which use the 
AWS default credentials chain (by default) for credentials and the region to construct an endpoint. For further 
information see the AWS Glue Schema Registry documentation.  

For the 3rd party Schema Registry, the location of the Schema Registry needs to be specified in a **producer.properties_msk** file.
   
For each event generated, the Producer assigns a **Global seq number** that is unique across the threads and sequential. 
The Producer assigns a **UserId** as the partition key for the events sent to Apache Kafka which means that the events 
for the same User always go to the same Kafka partition which would allow stateful processing of user events in order. 
However, the Global seq numbers are spread out across multiple partitions in Apache Kafka. 
The highest Global seq number received by the consumer at any point can be utilized to figure out how far behind the 
producer, the consumer is or if the consumer is caught up with the producer.

This producer supports TLS in-transit encryption, TLS mutual authentication and SASL/SCRAM authentication with Amazon MSK.
See the relevant parameters to enable them below.
   
This producer can be used to generate other types of events by modifying the RunProducer and Events classes.
   

## Install

This consumer depends on another library to get secrets from AWS Secrets Manager for SAS/SCRAM authentication with Amazon MSK.
The library needs to be installed first before creating the jar file for the consumer.

### Clone and install the jar file

    git clone https://github.com/aws-samples/sasl-scram-secrets-manager-client-for-msk.git
    cd sasl-scram-secrets-manager-client-for-msk
    mvn clean install -f pom.xml

### Clone the repository and install the jar file.  

    mvn clean package -f pom.xml
    
   ### The producer accepts a number of command line parameters:
   
   * ***--help (or -h): help to get list of parameters***
   * ***--numberOfUsers (or -nou)***: Specify the number of users sessions to generate. Default ***Integer.MAX_VALUE***.
   * ***--topic (or -t)***: Apache Kafka topic to send events to. Default ***ExampleTopic***.
   * ***--propertiesFilePath (or -pfp)***: Location of the producer properties file which contains information about the Apache Kafka bootstrap brokers and the location of the Confluent Schema Registry. Default ***/tmp/kafka/producer.properties***.
   * ***--numThreads (or -nt)***: Number of threads to run in parallel. Default ***2***.
   * ***--runFor (or -rf)***: Number of seconds to run the producer for.
   * ***--noDelay (or -nd)***: There is a built in delay between events in a user session to mimick a real user scenario. This turns it off to try and load events as fast as possible.
   * ***--sslEnable (or -ssl)***: Enable TLS communication between this application and Amazon MSK Apache Kafka brokers for in-transit encryption.
   * ***--mTLSEnable (or -mtls)***: Enable TLS communication between this application and Amazon MSK Apache Kafka brokers for in-transit encryption and TLS mutual authentication. If this parameter is specified, TLS is also enabled. This reads the specified properties file for **SSL_TRUSTSTORE_LOCATION_CONFIG**, **SSL_KEYSTORE_LOCATION_CONFIG**, **SSL_KEYSTORE_PASSWORD_CONFIG** and **SSL_KEY_PASSWORD_CONFIG**. Those properties need to be specified in the properties file.
   * ***--saslscramEnable (or -sse)***: Enable SASL/SCRAM authentication between this application and Amazon MSK with in-transit encryption. If this parameter is specified, --saslscramUser (or -ssu) also needs to be specified. Also, this parameter cannot be specified with --mTLSEnable (or -mtls) or --sslEnable (or -ssl)
   * ***--saslscramUser (or -ssu)***: The name of the SASL/SCRAM user stored in AWS Secrets Manager to be used for SASL/SCRAM authentication between this application and Amazon MSK. If this parameter is specified, --saslscramEnable (or -sse) also needs to be specified.
   * ***--region (or -reg)***: The region for AWS Secrets Manager storing secrets for SASL/SCRAM authentication with Amazon MSK. Default us-east-1.
   * ***--nologgingEvents (or -nle)***: Turn off logging of all events generated in a file in the filesystem. Default **false** and events are logged in a file **/tmp/Clickstream.txt**
   * ***--glueSchemaRegistry (or -gsr)***: Use the AWS Glue Schema Registry. Default **false**.
   * ***--gsrRegion or (-gsrr)***: The AWS region for the AWS Glue Schema Registry. Default ***us-east-1***.
   * ***--gsrRegistryName or (-grn)***: The AWS Glue Schema Registry Name.
   * ***--gsrSchemaName or (-gsn)***: The AWS Glue Schema Registry Schema Name. If not specified, the topic name is used as the schema name.
   * ***--gsrSchemaDescription or (-gsd)***: The AWS Glue Schema Registry Schema description. If not specified, a default one is generated.
   * ***--gsrAutoRegistration or (-gar)***: Turn on AWS Glue Schema Registry auto registration of schemas. Default **false**.
   * ***--gsrCompatibilitySetting or (-gcs)***: The compatibility setting for AWS Glue Schema Registry schema evolution. If not specified, uses the default setting for AWS Glue Schema Registry.
   
   ### Example usage with TLS mutual authentication:
   
   ```
   java -jar KafkaClickstreamClient-1.0-SNAPSHOT.jar -t ExampleTopic -pfp /tmp/kafka/producer.properties_msk -nt 8 -rf 300 -mtls
   ```

   ### Example usage with SASL/SCRAM authentication with a user called "nancy":
   
   ```
   java -jar KafkaClickstreamClient-1.0-SNAPSHOT.jar -t ExampleTopic -pfp /tmp/kafka/producer.properties_msk -nt 8 -rf 300 -sse ssu nancy
   ```
