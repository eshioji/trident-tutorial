package tutorial.storm.trident.operations;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import tutorial.storm.trident.testutil.Content;

/**
 * @author Davide Palmisano (davide.palmisano@peerindex.com)
 */
public class TweetIdExtractor extends BaseFunction {

    @Override
    public void execute(TridentTuple objects, TridentCollector tridentCollector) {
        Content content = (Content) objects.getValueByField("content");
        tridentCollector.emit(new Values(content.getTweetId()));
    }
}
