package tutorial.storm.trident.operations;

import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class ParseStatusJson extends BaseFunction {
    private static final Logger log = LoggerFactory.getLogger(ParseStatusJson.class);

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String tweetJson = tuple.getString(0);
        try {
            Status parsed = DataObjectFactory.createStatus(tweetJson);
            collector.emit(new Values(parsed));
        } catch (TwitterException e) {
            log.warn("Invalid tweet json -> " + tweetJson, e);
        }
    }
}
