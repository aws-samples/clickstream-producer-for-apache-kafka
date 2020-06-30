This is a Kafka Producer that generates mock Clickstream data for an imaginary e-commerce site. The scenario is that a user logs into the site, lands on the home page, browses the product catalog, lands on individual product pages, and either adds the product to cart or not and conitues to do that until the user either creates an order and does order checkout or abandons the session.

   
The producer generates the events in a user session in sequence but runs multiple threads to simulate multiple users hitting the site. It can utilize multiple threads to parallelize event generation and the number of threads can be specified by using parameters. It utilizes the Schema Registry and generates Avro encoded events. Consequently, the location of the Schema Registry needs to be specified in a **producer.properties_msk** file. 
   For each event generated, the Producer assigns a **Global seq number** that is unique across the threads and sequential. The Producer assigns a **UserId** as the partition key for the events sent to Apache Kafka which means that the events for the same User always go to the same Kafka partition which would allow stateful processing of user events in order. However, the Global seq numbers are spread out across multiple partitions in Apache Kafka. The highest Global seq number received by the consumer at any point can be utilized to figure out how far behind the producer, the consumer is or if the consumer is caught up with the producer.
   
This producer can be used to generate other types of events by modifying the RunProducer and Events classes.
   

## Install

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
   * ***--nologgingEvents (or -nle)***: Turn off logging of all events generated in a file in the filesystem. Default **false** and events are logged in a file **/tmp/Clickstream.txt**
   
   ### Example usage:
   
   ```
   java -jar KafkaClickstreamClient-1.0-SNAPSHOT.jar -t ExampleTopic -pfp /tmp/kafka/producer.properties_msk -nt 8 -rf 300 -mtls
   ```
