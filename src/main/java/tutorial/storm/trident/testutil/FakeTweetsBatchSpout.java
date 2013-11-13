package tutorial.storm.trident.testutil;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Spout that emits fake tweets.
 * @author pere
 * @author Modified by Enno Shioji (enno.shioji@peerindex.com)
 */
@SuppressWarnings({"serial", "rawtypes"})
public class FakeTweetsBatchSpout implements IBatchSpout {
    private static final AtomicInteger seq = new AtomicInteger();

    private int batchSize;
    private FakeTweetGenerator fakeTweetGenerator;


    public FakeTweetsBatchSpout() throws IOException {
        this(5);
    }

    public FakeTweetsBatchSpout(int batchSize) throws IOException {
        this.batchSize = batchSize;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void open(Map conf, TopologyContext context) {
        // init
        System.err.println(this.getClass().getSimpleName() +":" + seq.incrementAndGet()+" opened.");
        fakeTweetGenerator = new FakeTweetGenerator();

    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        // emit batchSize fake tweets
        for (int i = 0; i < batchSize; i++) {
            collector.emit(fakeTweetGenerator.getNextTweet());
        }
    }

    @Override
    public void ack(long batchId) {
        // nothing to do here
    }

    @Override
    public void close() {
        // nothing to do here
    }

    @Override
    public Map getComponentConfiguration() {
        // no particular configuration here
        return new Config();
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("id", "text", "actor", "location", "date");
    }
}
