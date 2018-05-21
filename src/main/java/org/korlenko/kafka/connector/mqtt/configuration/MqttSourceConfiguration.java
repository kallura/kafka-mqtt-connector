package org.korlenko.kafka.connector.mqtt.configuration;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.korlenko.kafka.connector.mqtt.processor.impl.MqttJsonProcessor;

import java.util.Map;


/**
 * MQTT connector configuration class
 *
 * @author Kseniia Orlenko
 * @version 5/18/18
 */
public class MqttSourceConfiguration extends AbstractConfig {

    public static final String MQTT_CLIENT_ID = "mqtt.client_id";
    public static final String MQTT_CLEAN_SESSION = "mqtt.clean_session";
    public static final String MQTT_CONNECTION_TIMEOUT = "mqtt.connection_timeout";
    public static final String MQTT_KEEP_ALIVE_INTERVAL = "mqtt.keep_alive_interval";
    public static final String MQTT_SERVER_URIS = "mqtt.server_uris";
    public static final String MQTT_TOPIC = "mqtt.topic";
    public static final String MQTT_QUALITY_OF_SERVICE = "mqtt.qos";
    public static final String MESSAGE_PROCESSOR_CLASS = "message_processor_class";
    public static final String KAFKA_TOPIC_TEMPLATE = "kafka.%s.topic";


    private static final ConfigDef CONFIG_DEF = new ConfigDef();
    private final Map<String, String> props;

    static {
        CONFIG_DEF
                .define(MQTT_CLEAN_SESSION, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH,
                        "Enabling session clean-up")
                .define(MQTT_SERVER_URIS, ConfigDef.Type.STRING, "tcp://localhost:1883",
                        ConfigDef.Importance.HIGH, "MQTT server url")
                .define(MQTT_TOPIC, ConfigDef.Type.STRING, "#", ConfigDef.Importance.HIGH,
                        "MQTT topic")
                .define(MESSAGE_PROCESSOR_CLASS, ConfigDef.Type.CLASS, MqttJsonProcessor.class,
                        ConfigDef.Importance.HIGH, "Processor")
                .define(MQTT_CLIENT_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                        "MQTT client id")
                .define(MQTT_CONNECTION_TIMEOUT, ConfigDef.Type.INT, 30, ConfigDef.Importance.HIGH,
                        "Connection timeout")
                .define(MQTT_KEEP_ALIVE_INTERVAL, ConfigDef.Type.INT, 60, ConfigDef.Importance.HIGH,
                        "Keep alive interval")
                .define(MQTT_QUALITY_OF_SERVICE, ConfigDef.Type.INT, 1, ConfigDef.Importance.HIGH,
                        "MQTT qos");
    }

    /**
     * Checks row key property is configured per table
     */
    public void validate() {
        final String topicsAsStr = props.get(MQTT_TOPIC);
        final String[] topics = topicsAsStr.split(",");
        for (String topic : topics) {
            String key = String.format(KAFKA_TOPIC_TEMPLATE, topic);
            System.out.println(key);
            if (!props.containsKey(key)) {
                throw new ConfigException(
                        String.format(" No topic has been found for [%s]", key));
            }
        }
    }

    public MqttSourceConfiguration(Map<String, String> props) {
        super(CONFIG_DEF, props);
        this.props = props;
    }

    /**
     * Creates default config
     *
     * @return default config
     */
    public static ConfigDef getConfigDef() {
        return CONFIG_DEF;
    }
}
