package org.korlenko.kafka.connector.mqtt.helper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Loads the project version from the property file
 *
 * @author Kseniia Orlenko
 * @version 5/18/18
 */
public class VersionHelper {

    private final static Logger logger = LoggerFactory.getLogger(VersionHelper.class);
    private final static Properties properties = new Properties();

    static {
        try {
            properties.load(VersionHelper.class.getResourceAsStream("/application.properties"));
        } catch (IOException e) {
            logger.warn("Can't load the project properties");
        }
    }

    public static String getProjectVersion() {
        return properties.getProperty("project.version");
    }
}
