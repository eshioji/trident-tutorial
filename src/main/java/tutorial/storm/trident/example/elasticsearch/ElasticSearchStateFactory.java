package tutorial.storm.trident.example.elasticsearch;

import backtype.storm.task.IMetricsContext;
import org.elasticsearch.client.Client;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * This implementation is basically what Trident needs to build your custom
 * {@link State} implementation.
 *
 * @author Davide Palmisano (davide.palmisano@peerindex.com)
 */
public class ElasticSearchStateFactory implements StateFactory {

    @Override
    public State makeState(Map map, IMetricsContext iMetricsContext, int partitionIndex, int numPartitions) {
        /**
         * Here, we're using a singleton because we're connecting to a local in-memory ES-cluster.
         * In a real world application, you should just instantiate a client to connect
         * to your production database.
         */
        Client client = ElasticSearchSingleton.getInstance();
        return new ElasticSearchState(client);
    }
}
