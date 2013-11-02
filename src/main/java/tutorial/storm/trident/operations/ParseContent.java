package tutorial.storm.trident.operations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import twitter4j.Status;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class ParseContent extends BaseFunction {
    private static final Logger log = LoggerFactory.getLogger(ParseContent.class);

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Status status = (Status)tuple.get(0);
        status.
    }
}
