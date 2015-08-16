package com.amazonaws.services.logs.connectors.samples.founddotio;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.*;
import com.amazonaws.services.logs.connectors.elasticsearch.ElasticsearchTransformer;
import com.amazonaws.services.logs.subscriptions.CloudWatchLogsEvent;

/**
 * Created by ben on 16/08/15.
 */
public class FounddotioPipeline implements IKinesisConnectorPipeline<CloudWatchLogsEvent, ElasticsearchObject> {
    @Override
    public IEmitter<ElasticsearchObject> getEmitter(KinesisConnectorConfiguration configuration) {
        return new FounddotioEmitter(configuration);
    }

    @Override
    public IBuffer<CloudWatchLogsEvent> getBuffer(KinesisConnectorConfiguration configuration) {
        // a very basic in-heap buffer
        return new BasicMemoryBuffer<CloudWatchLogsEvent>(configuration);
    }

    @Override
    public ITransformerBase<CloudWatchLogsEvent, ElasticsearchObject> getTransformer(KinesisConnectorConfiguration configuration) {
        return new ElasticsearchTransformer();

    }

    @Override
    public IFilter<CloudWatchLogsEvent> getFilter(KinesisConnectorConfiguration configuration) {
// no filtering
        return new AllPassFilter<CloudWatchLogsEvent>();
    }
}
