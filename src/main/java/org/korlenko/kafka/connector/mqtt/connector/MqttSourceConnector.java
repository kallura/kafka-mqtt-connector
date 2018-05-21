package org.korlenko.kafka.connector.mqtt.connector;

import static org.korlenko.kafka.connector.mqtt.configuration.MqttSourceConfiguration.getConfigDef;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.korlenko.kafka.connector.mqtt.helper.VersionHelper;
import org.korlenko.kafka.connector.mqtt.task.MqttSourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MQTT source connector
 *
 * @author Kseniia Orlenko
 * @version 5/18/18
 */
public class MqttSourceConnector extends SourceConnector {

    private static final Logger logger = LoggerFactory.getLogger(MqttSourceConnector.class);

    private final ConfigDef config = getConfigDef();
    private Map<String, String> props;

    /**
     * Returns the connector version
     *
     * @return the version
     */
    public String version() {
        return VersionHelper.getProjectVersion();
    }

    /**
     * Starts the connector
     *
     * @param props configuration settings
     */
    public void start(Map<String, String> props) {
        logger.info("Start the MQTT source connector");
        this.props = props;
    }

    /**
     * Returns source task implementation
     *
     * @return SourceTask class instance
     */
    public Class<? extends Task> taskClass() {
        return MqttSourceTask.class;
    }

    /**
     * Returns tasks configurations
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for tasks
     */
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskProps = new HashMap<>(props);
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    /**
     * Stops the connector
     */
    public void stop() {
        logger.info("Stop the MQTT source connector");
    }

    /**
     * Returns the config
     *
     * @return default config
     */
    public ConfigDef config() {
        return config;
    }
}
