package tutorial.storm.trident.operations;

import backtype.storm.tuple.Values;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import tutorial.storm.trident.testutil.Content;
import twitter4j.User;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class ExtractFollowerClassAndContentName extends BaseFunction {
    private RangeMap<Integer, String> discretizationMap;
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        if(discretizationMap == null) discretizationMap = getDiscretizationMap();

        Content content = (Content)tuple.get(0);
        User user = (User)tuple.get(1);

        String followerClass = discretizationMap.get(user.getFollowersCount());

        collector.emit(new Values(followerClass, content.getContentName()));
    }

    private RangeMap<Integer, String> getDiscretizationMap() {
        ImmutableRangeMap.Builder<Integer,String> b = ImmutableRangeMap.builder();
        b.put(  Range.lessThan(10),                                     "< 10");
        b.put(Range.closedOpen(10,                 100),                "10-100");
        b.put(Range.closedOpen(100,               1000),                "100-1K");
        b.put(Range.closedOpen(1000,         10 * 1000),                "1K-10K");
        b.put(Range.closedOpen(10 * 1000,   100 * 1000),                "10K-100K");
        b.put(Range.closedOpen(100 * 1000, 1000 * 1000),                "100K-1M");
        b.put(Range.greaterThan(1000 * 1000),                           "> 1M");
        return b.build();
    }
}
