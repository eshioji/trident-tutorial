package tutorial.storm.trident.example.elasticsearch;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Trident needs to know how to access your custom state when querying it.
 * This implementation is what we need to query ElasticSearch.
 *
 * Queries are made in two steps: batchRetrieve (1) and execute (2). See below.
 *
 * PS: please note, that size of inputs for step (2) must be of the same size of the
 * returned values of step (1).
 *
 * @author Davide Palmisano (davide.palmisano@peerindex.com)
 */
public class TweetQuery extends BaseQueryFunction<ElasticSearchState, Set<String>> {

    /**
     * Step (1)
     *
     * The arguments, passed as a list of tuples, are passed to this method.
     * Here is where you retrieve the results from your state. In our case is
     * simply a query to ElasticSearch.
     *
     * @param state
     * @param inputs
     * @return
     */
    @Override
    public List<Set<String>> batchRetrieve(ElasticSearchState state, List<TridentTuple> inputs) {
        List<Set<String>> keywords = new ArrayList<Set<String>>();
        for(TridentTuple input: inputs) {
            Set<String> matches = state.search(input.getString(0));
            keywords.add(matches);
        }
        return keywords;
    }

    /**
     * Step (2)
     *
     * Then trident calls this step for each item returned from step (1). In our
     * case each item is made of a set of tweet ids matching the keywords we queries for.
     *
     * Here, we simply emit them.
     *
     * @param objects
     * @param matches
     * @param collector
     */
    @Override
    public void execute(TridentTuple objects, Set<String> matches, TridentCollector collector) {
        for(String match : matches) collector.emit(new Values(match));
    }
}
