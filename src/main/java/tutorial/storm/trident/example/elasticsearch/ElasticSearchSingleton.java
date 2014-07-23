package tutorial.storm.trident.example.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * @author Davide Palmisano (davide.palmisano@peerindex.com)
 */
public class ElasticSearchSingleton {

    private static final Client INSTANCE = getInstance();

    public static Client getInstance() {
            Node node = nodeBuilder().node();
            Client client = node.client();
            createIndex(INSTANCE);
            return client;
    }

    private static void createIndex(Client client) {
        boolean exist = client.admin().indices().prepareExists("hackaton").execute().actionGet().isExists();
        if (!exist) {
            client.admin().indices().prepareCreate("hackaton").execute().actionGet();
        }
    }

}
