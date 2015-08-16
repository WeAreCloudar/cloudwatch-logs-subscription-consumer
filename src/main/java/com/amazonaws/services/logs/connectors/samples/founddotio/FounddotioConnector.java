package com.amazonaws.services.logs.connectors.samples.founddotio;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory;
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.logs.subscriptions.CloudWatchLogsEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class FounddotioConnector extends KinesisConnectorExecutorBase<CloudWatchLogsEvent, ElasticsearchObject>{

    private static final Log LOG = LogFactory.getLog(FounddotioConnector.class);
    private final FounddotioKinesisConnectorConfiguration config;



    private static String CONFIG_FILE = FounddotioConnector.class.getSimpleName() + ".properties";
    /**
     * Creates a new CloudWatchLogsSubscriptionsExecutor.
     *
     * @param configFile The name of the configuration file to look for on the classpath.
     */
    public FounddotioConnector(String configFile) {
        InputStream configFileInputStream = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(configFile);

        if (configFileInputStream == null) {
            String msg = "Could not find resource " + configFile + " in the classpath";
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }

        Properties propertiesFile = new Properties();
        Properties mergedProperties = new Properties();
        try {
            propertiesFile.load(configFileInputStream);

            mergedProperties.putAll(propertiesFile);
            mergedProperties.putAll(System.getProperties());

            configFileInputStream.close();

            this.config = new FounddotioKinesisConnectorConfiguration(mergedProperties, new DefaultAWSCredentialsProviderChain());
        } catch (IOException e) {
            String msg = "Could not load properties file " + configFile + " from classpath";
            LOG.error(msg, e);
            throw new IllegalStateException(msg, e);
        }

        super.initialize(config, new NullMetricsFactory());
    }

    @Override
    public KinesisConnectorRecordProcessorFactory<CloudWatchLogsEvent, ElasticsearchObject> getKinesisConnectorRecordProcessorFactory() {
        return new KinesisConnectorRecordProcessorFactory<CloudWatchLogsEvent, ElasticsearchObject>(new FounddotioPipeline(), getConfig());
    }

    protected KinesisConnectorConfiguration getConfig() {
        return config;
    }


    public static void main(String[] args) {
        FounddotioConnector executor = new FounddotioConnector(CONFIG_FILE);
        executor.run();
    }
}
