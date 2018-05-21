package org.korlenko.kafka.connector.mqtt.task;

import static org.korlenko.kafka.connector.mqtt.configuration.MqttSourceConfiguration.KAFKA_TOPIC_TEMPLATE;
import static org.korlenko.kafka.connector.mqtt.configuration.MqttSourceConfiguration.MESSAGE_PROCESSOR_CLASS;
import static org.korlenko.kafka.connector.mqtt.configuration.MqttSourceConfiguration.MQTT_CLEAN_SESSION;
import static org.korlenko.kafka.connector.mqtt.configuration.MqttSourceConfiguration.MQTT_CLIENT_ID;
import static org.korlenko.kafka.connector.mqtt.configuration.MqttSourceConfiguration.MQTT_CONNECTION_TIMEOUT;
import static org.korlenko.kafka.connector.mqtt.configuration.MqttSourceConfiguration.MQTT_KEEP_ALIVE_INTERVAL;
import static org.korlenko.kafka.connector.mqtt.configuration.MqttSourceConfiguration.MQTT_QUALITY_OF_SERVICE;
import static org.korlenko.kafka.connector.mqtt.configuration.MqttSourceConfiguration.MQTT_SERVER_URIS;
import static org.korlenko.kafka.connector.mqtt.configuration.MqttSourceConfiguration.MQTT_TOPIC;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.korlenko.kafka.connector.mqtt.configuration.MqttSourceConfiguration;
import org.korlenko.kafka.connector.mqtt.helper.VersionHelper;
import org.korlenko.kafka.connector.mqtt.processor.MqttProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Kafka connect source task implementation that reads from MQTT and generates Kafka connect records
 *
 * @author Kseniia Orlenko
 * @version 5/18/18
 */
public class MqttSourceTask extends SourceTask implements MqttCallback {

    private static final Logger logger = LoggerFactory.getLogger(MqttSourceTask.class);
    private final BlockingQueue<MqttProcessor> mqttMessages = new LinkedBlockingQueue<>();

    private MqttSourceConfiguration configuration;
    private Map<String, String> props;
    private MqttClient mqttClient;
    private String mqttClientId;

    /**
     * Returns the task version
     *
     * @return the version
     */
    public String version() {
        return VersionHelper.getProjectVersion();
    }

    /**
     * Starts the task.
     *
     * @param props initial configuration
     */
    public void start(Map<String, String> props) {
        this.props = props;
        this.configuration = new MqttSourceConfiguration(props);
        configuration.validate();
        String mqttBroker = configuration.getString(MQTT_SERVER_URIS);
        this.mqttClientId = configuration.getString(MQTT_CLIENT_ID);
        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(configuration.getBoolean(MQTT_CLEAN_SESSION));
        connectOptions.setConnectionTimeout(configuration.getInt(MQTT_CONNECTION_TIMEOUT));
        connectOptions.setKeepAliveInterval(configuration.getInt(MQTT_KEEP_ALIVE_INTERVAL));
        connectOptions.setServerURIs(configuration.getString(MQTT_SERVER_URIS).split(","));
        try {
            mqttClient = new MqttClient(mqttBroker, mqttClientId, new MemoryPersistence());
            mqttClient.setCallback(this);
            mqttClient.connect(connectOptions);
            logger.info("Connected to Broker", mqttClientId);
        } catch (MqttException e) {
            logger.error("[Connection to Broker failed!", mqttClientId, e);

        }
        try {
            String[] mqttTopics = configuration.getString(MQTT_TOPIC).split(",");
            Integer qos = configuration.getInt(MQTT_QUALITY_OF_SERVICE);
            for (String mqttTopic : mqttTopics) {
                mqttClient.subscribe(mqttTopic, qos);
                logger.info("Subscribe to '{}' with QoS '{}'", mqttClientId, mqttTopic, qos);
            }
        } catch (MqttException e) {
            logger.error("Subscribe failed! ", mqttClientId, e);
        }
    }

    /**
     * Polls the source task for new records
     * The method is blocked in case if no data in mqttMessages
     *
     * @return a source records list
     * @throws InterruptedException thread is waiting, sleeping, or otherwise occupied,
     *                              and the thread is interrupted, either before or during the
     *                              activity
     */
    public List<SourceRecord> poll() throws InterruptedException {
        logger.debug("Polling new data from queue for '{}' topics.", mqttClientId);

        List<SourceRecord> records = new ArrayList<>();
        MqttProcessor message = mqttMessages.take();
        String kafkaTopic = props.get(String.format(KAFKA_TOPIC_TEMPLATE, message.getTopic()));
        records.add(message.getRecords(kafkaTopic));
        return records;
    }

    /**
     * Stop the task.
     */
    public void stop() {
        logger.info("Stopping the MqttSourceTask");
        try {
            mqttClient.disconnect();
            logger.info("Disconnected from Broker.", mqttClientId);
        } catch (MqttException e) {
            logger.error("Disconnecting from Broker failed!", mqttClientId, e);
        }
    }

    /**
     * This method is called when the connection to the server is lost
     *
     * @param throwable the reason behind the loss of connection.
     */
    public void connectionLost(Throwable throwable) {
        logger.error("MQTT connection lost!", throwable);
    }

    /**
     * This method is called when a message arrives from the server.
     *
     * @param topic       name of the topic on the message was published to
     * @param mqttMessage the actual message.
     */
    public void messageArrived(String topic, MqttMessage mqttMessage) {
        logger.debug("New message on '{}' arrived.", mqttClientId, topic);
        mqttMessages.add(
                configuration.getConfiguredInstance(MESSAGE_PROCESSOR_CLASS, MqttProcessor.class)
                        .process(topic, mqttMessage)
        );
    }

    /**
     * Called when delivery for a message has been completed, and all acknowledgments have been
     * received.
     *
     * @param iMqttDeliveryToken the delivery token associated with the message.
     */
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
    }
}
