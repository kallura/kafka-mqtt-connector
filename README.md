## Kafka MQTT connector

This is a very simple Kafka source connector to MQTT broker

#### Prerequisites
1. Java v1.8
2. Kafka v1.1.0
3. Maven v3.5.2

#### Installing

Start up your MQTT broker with preconfigured topics and producers

Start Kafka
````
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties 
````
Build the project
````
mvn clean install
````
Copy the jars to ${kafka_home}/libs
````
kafka-mqtt-connector-1.0-SNAPSHOT-jar-with-dependencies.jar
````
Add connector properties to ${kafka_home}/config
````
name=mqtt-power
connector.class=org.korlenko.kafka.connector.mqtt.connector.MqttSourceConnector
tasks.max=1
mqtt.qos=1
kafka.power.topic=power
kafka.energy.topic=energy
kafka.temperature.topic=temperature
mqtt.client_id=mqtt-kafka-power
mqtt.clean_session=true
mqtt.connection_timeout=30
mqtt.keep_alive_interval=60
mqtt.server_uris=tcp://127.0.0.1:1883
mqtt.topic=power,energy,temperature
message_processor_class=org.korlenko.kafka.connector.mqtt.processor.impl.MqttJsonProcessor

````
Run connector
````
 bin/connect-standalone.sh config/connect-standalone.properties config/mqtt.properties
````
See the result
````
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic test
````

#### todo

Add SSL

Add tests