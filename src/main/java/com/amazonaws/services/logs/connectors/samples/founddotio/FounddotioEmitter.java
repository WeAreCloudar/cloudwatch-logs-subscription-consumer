package com.amazonaws.services.logs.connectors.samples.founddotio;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class FounddotioEmitter implements IEmitter<ElasticsearchObject> {

    private static final Log LOG = LogFactory.getLog(FounddotioEmitter.class);


    private final TransportClient founddotioClient;


    /**
     * The amount of time to wait in between unsuccessful index requests (in milliseconds).
     * 10 seconds = 10 * 1000 = 10000
     */
    private long BACKOFF_PERIOD = 10000;
    private final String founddotioEndpoint;
    private final int founddotioPort;

    public FounddotioEmitter(KinesisConnectorConfiguration configuration) {


        FounddotioKinesisConnectorConfiguration config = (FounddotioKinesisConnectorConfiguration) configuration;

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("transport.type", "org.elasticsearch.transport.netty.FoundNettyTransport")
                .put("transport.found.api-key", config.ELASTICSEARCH_API_KEY)
                .put("cluster.name", config.ELASTICSEARCH_CLUSTER_NAME)
                .put("client.transport.ignore_cluster_name", config.ELASTICSEARCH_IGNORE_CLUSTER_NAME)

                .build();


        // variables because they're also used for debug messages
        founddotioEndpoint = configuration.ELASTICSEARCH_ENDPOINT;
        founddotioPort = configuration.ELASTICSEARCH_PORT;

        founddotioClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(founddotioEndpoint, founddotioPort));




    }


    @Override
    public List<ElasticsearchObject> emit(UnmodifiableBuffer<ElasticsearchObject> buffer) throws IOException {
        List<ElasticsearchObject> records = buffer.getRecords();
        if (records.isEmpty()) {
            return Collections.emptyList();
        }

        BulkRequestBuilder bulkRequest = founddotioClient.prepareBulk();
        for (ElasticsearchObject record : records) {
            IndexRequestBuilder indexRequestBuilder =
                    founddotioClient.prepareIndex(record.getIndex(), record.getType(), record.getId());
            indexRequestBuilder.setSource(record.getSource());
            Long version = record.getVersion();
            if (version != null) {
                indexRequestBuilder.setVersion(version);
            }
            Long ttl = record.getTtl();
            if (ttl != null) {
                indexRequestBuilder.setTTL(ttl);
            }
            Boolean create = record.getCreate();
            if (create != null) {
                indexRequestBuilder.setCreate(create);
            }
            bulkRequest.add(indexRequestBuilder);
        }

        while (true) {
            try {
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();

                BulkItemResponse[] responses = bulkResponse.getItems();
                List<ElasticsearchObject> failures = new ArrayList<ElasticsearchObject>();
                int numberOfSkippedRecords = 0;
                for (int i = 0; i < responses.length; i++) {
                    if (responses[i].isFailed()) {
                        LOG.error("Record failed with message: " + responses[i].getFailureMessage());
                        BulkItemResponse.Failure failure = responses[i].getFailure();
                        if (failure.getMessage().contains("DocumentAlreadyExistsException")
                                || failure.getMessage().contains("VersionConflictEngineException")) {
                            numberOfSkippedRecords++;
                        } else {
                            failures.add(records.get(i));
                        }
                    }
                }
                LOG.info("Emitted " + (records.size() - failures.size() - numberOfSkippedRecords)
                        + " records to Elasticsearch");
                if (!failures.isEmpty()) {
                    printClusterStatus();
                    LOG.warn("Returning " + failures.size() + " records as failed");
                }
                return failures;
            } catch (NoNodeAvailableException nnae) {
                LOG.error("No nodes found at " + founddotioEndpoint + ":" + founddotioPort + ". Retrying in "
                        + BACKOFF_PERIOD + " milliseconds", nnae);
                sleep(BACKOFF_PERIOD);
            } catch (Exception e) {
                LOG.error("ElasticsearchEmitter threw an unexpected exception ", e);
                sleep(BACKOFF_PERIOD);
            }
        }

    }

    @Override
    public void fail(List<ElasticsearchObject> records) {
        for (ElasticsearchObject record : records) {
            LOG.error("Record failed: " + record);
        }
    }

    @Override
    public void shutdown() {
        founddotioClient.close();
    }


    private void printClusterStatus() {
        ClusterHealthRequestBuilder healthRequestBuilder = founddotioClient.admin().cluster().prepareHealth();
        ClusterHealthResponse response = healthRequestBuilder.execute().actionGet();
        if (response.getStatus().equals(ClusterHealthStatus.RED)) {
            LOG.error("Cluster health is RED. Indexing ability will be limited");
        } else if (response.getStatus().equals(ClusterHealthStatus.YELLOW)) {
            LOG.warn("Cluster health is YELLOW.");
        } else if (response.getStatus().equals(ClusterHealthStatus.GREEN)) {
            LOG.info("Cluster health is GREEN.");
        }
    }

    private void sleep(long sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
        }
    }

}
