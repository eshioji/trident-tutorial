package tutorial.storm.trident.example.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * @author Davide Palmisano (davide.palmisano@peerindex.com)
 */
public class ElasticSearchSingleton {

    private static Client INSTANCE;

    public synchronized static Client getInstance() {
        if(INSTANCE == null) {
            Node node = nodeBuilder().node();
            INSTANCE = node.client();
            createIndex(INSTANCE);
        }
        return INSTANCE;
    }

    private static void createIndex(Client client) {
        boolean exist = client.admin().indices().prepareExists("hackaton").execute().actionGet().isExists();
        if (!exist) {
            client.admin().indices().prepareCreate("hackaton").execute().actionGet();
        }
    }

}
