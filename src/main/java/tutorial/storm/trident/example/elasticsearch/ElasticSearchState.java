package tutorial.storm.trident.example.elasticsearch;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import storm.trident.state.State;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * This implementation of {@link State} basically provides facilities to
 * index and search on stored tweets.
 *
 * @author Davide Palmisano (davide.palmisano@peerindex.com)
 */
public class ElasticSearchState implements State {

    private Client client;

    public ElasticSearchState(Client client) {
        this.client = client;
    }

    /**
     * We're not using bulk transaction ids here. But basically, this is
     * called by Trident when starting operations on a bulk.
     *
     * @param txid
     */
    @Override
    public void beginCommit(Long txid) {}

    /**
     * We're not using bulk transaction ids here, but this method is called by
     * Trident when a bulk transaction is done.
     *
     * @param txid
     */
    @Override
    public void commit(Long txid) {}

    /**
     * Given that streams are processed in bulks, we're making use of
     * the ElasticSearch capability to index bulks of documents. It takes a list
     * of ids and a list of texts.
     *
     * @param tweetIds
     * @param tweets
     */
    public void bulkIndex(List<Long> tweetIds, List<String> tweets) {
        BulkRequestBuilder requestBuilder = client.prepareBulk();
        for(int i = 0; i < tweetIds.size(); i++) {
            XContentBuilder builder;
            try {
                builder = jsonBuilder()
                        .startObject()
                        .field("text", tweets.get(i))
                        .field("id", tweetIds.get(i))
                        .endObject();
            } catch (IOException e) {
                continue;
            }
            IndexRequestBuilder request = client.prepareIndex("hackaton", "tweets")
                    .setIndex("hackaton")
                    .setType("tweets")
                    .setSource(builder);
            requestBuilder.add(request);
        }
        BulkResponse bulkResponse = requestBuilder.execute().actionGet();
        int items = bulkResponse.getItems().length;
        System.err.print("indexed [" + items + "] items, with failures? [" + bulkResponse.hasFailures()  + "]");
    }

    /**
     * It basically searches and returns a set of tweet ids for a given keyword.
     *
     * @param keyword
     * @return
     */
    public Set<String> search(String keyword) {
        SearchResponse response;
        try {
            response = client.prepareSearch()
                    .setIndices("hackaton")
                    .setTypes("tweets")
                    .addFields("id", "text")
                    .setQuery(QueryBuilders.fieldQuery("text", keyword)).execute().actionGet();
        } catch (Throwable e) {
            return new HashSet<String>();
        }
        Set<String> result = new HashSet<String>();
        for (SearchHit hit : response.getHits()) {
            Long id = hit.field("id").<Long>getValue();
            result.add(String.valueOf(id));
        }
        return result;
    }


}
