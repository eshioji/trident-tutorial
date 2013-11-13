package tutorial.storm.trident.example.elasticsearch;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * An updater is a class used by Trident to update a state. This is an implementation
 * for our ElasticSearch use case.
 *
 * @author Davide Palmisano (davide.palmisano@peerindex.com)
 */
public class ElasticSearchStateUpdater extends BaseStateUpdater<ElasticSearchState> {

    /**
     * What it basically does is to grab the tweetId and its text, put them into two lists to
     * execute the bulk indexing.
     *
     * @param state
     * @param tuples
     * @param collector
     */
    @Override
    public void updateState(ElasticSearchState state, List<TridentTuple> tuples, TridentCollector collector) {
        List<Long> ids = new ArrayList<Long>();
        List<String> tweets = new ArrayList<String>();
        for(TridentTuple t: tuples) {
            ids.add(t.getLong(0)); // get the tweetId
            tweets.add(t.getString(1)); // get the text of the tweet
        }
        state.bulkIndex(ids, tweets);
    }
}
