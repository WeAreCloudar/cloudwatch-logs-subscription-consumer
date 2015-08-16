package com.amazonaws.services.logs.connectors.samples.founddotio;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;

import java.util.Properties;

/**
 * Created by ben on 16/08/15.
 */
public class FounddotioKinesisConnectorConfiguration extends KinesisConnectorConfiguration {
    /**
     * Configure the connector application with any set of properties that are unique to the application. Any
     * unspecified property will be set to a default value.
     *
     * @param properties          the System properties that will be used to configure KinesisConnectors
     * @param credentialsProvider
     */

    public static final String PROP_ELASTICSEARCH_API_KEY = "elasticsearchApiKey";

    public final String ELASTICSEARCH_API_KEY;

    public FounddotioKinesisConnectorConfiguration(Properties properties, AWSCredentialsProvider credentialsProvider) {
        super(properties, credentialsProvider);

        ELASTICSEARCH_API_KEY = properties.getProperty(PROP_ELASTICSEARCH_API_KEY);
        ELASTICSEARCH_ENDPOINT = properties.getProperty(PROP_ELASTICSEARCH_ENDPOINT, ELASTICSEARCH_CLUSTER_NAME + "." + REGION_NAME + ".aws.found.io");
    }
}
