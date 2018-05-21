package org.korlenko.kafka.connector.mqtt.processor.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.korlenko.kafka.connector.mqtt.processor.MqttProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Json processor implementation
 *
 * @author Kseniia Orlenko
 * @version 5/18/18
 */
public class MqttJsonProcessor implements MqttProcessor {

    private static final Logger loggger = LoggerFactory.getLogger(MqttJsonProcessor.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    private MqttMessage message;
    private String topic;

    @Override
    public MqttProcessor process(String topic, MqttMessage message) {
        loggger.info("Processing data topic: [{}]; message [{}]", topic, message);
        this.message = message;
        this.topic = topic;
        return this;
    }

    /**
     * Transforms MQTT payload from byte[] to json string, creates the SourceRecord and returns it
     *
     * @param #kafkaTopic the kafka topic
     * @return the SourceRecord
     */
    @Override
    public SourceRecord getRecords(String kafkaTopic) {
        String toString = null;
        try {
            Map<String, Object> fromBytes = objectMapper.readValue(message.getPayload(),
                    new TypeReference<Map<String, Object>>() {
                    });
            toString = objectMapper.writeValueAsString(fromBytes);
        } catch (IOException e) {
            loggger.error("Fail to map a message, error : {}", e);
        }
        return new SourceRecord(null, null, kafkaTopic, null,
                Schema.STRING_SCHEMA, kafkaTopic,
                Schema.STRING_SCHEMA, toString);
    }

    @Override
    public String getTopic() {
        return topic;
    }


}
