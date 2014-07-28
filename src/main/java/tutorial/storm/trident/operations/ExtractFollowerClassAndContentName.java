package tutorial.storm.trident.operations;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import tutorial.storm.trident.testutil.Content;
import twitter4j.User;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class ExtractFollowerClassAndContentName extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Content content = (Content)tuple.get(0);
        User user = (User)tuple.get(1);

        String followerClass = classify(user.getFollowersCount());

        collector.emit(new Values(followerClass, content.getContentName()));
    }

    private String classify(int followersCount) {
        if (followersCount < 100){
            return "< 100";
        } else if (followersCount < 10*1000){
            return "< 10K";
        } else if (followersCount < 100*1000){
            return "< 100K";
        } else {
            return ">= 100K";
        }
    }

}
