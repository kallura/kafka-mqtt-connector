package org.korlenko.kafka.connector.mqtt.processor;

import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * MQTT processor interface should be implemented by all processors
 *
 * @author Kseniia Orlenko
 * @version 5/18/18
 */
public interface MqttProcessor {

    /**
     * Processes a message when the message arrive from MQTT broker topic
     *
     * @param #topic   the MQTT topic
     * @param #message the message from MQTT topic
     * @return the MqttProcessor
     */
    MqttProcessor process(String topic, MqttMessage message);

    /**
     * Returns the SourceRecord for send to kafka topic
     *
     * @param #kafkaTopic the kafka topic
     * @return the SourceRecord
     */
    SourceRecord getRecords(String kafkaTopic);

    /**
     * Returns the mqtt source topic
     *
     * @return the mqtt source topic
     */
    String getTopic();
}
