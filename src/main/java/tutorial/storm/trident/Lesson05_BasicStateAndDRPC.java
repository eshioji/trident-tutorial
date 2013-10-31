package tutorial.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.*;
import storm.trident.testing.FeederBatchSpout;
import storm.trident.testing.MemoryMapState;
import tutorial.storm.trident.operations.RegexFilter;
import tutorial.storm.trident.operations.Split;
import tutorial.storm.trident.testutil.FakeTweetGenerator;

import java.io.IOException;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class Lesson05_BasicStateAndDRPC {
    private static final Logger log = LoggerFactory.getLogger(Lesson05_BasicStateAndDRPC.class);

    public static void main(String[] args) throws Exception{
        FakeTweetGenerator fakeTweets = new FakeTweetGenerator();
        FeederBatchSpout testSpout = new FeederBatchSpout(ImmutableList.of("id", "text", "actor", "location", "date"));

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology("state_drpc", conf, basicStateAndDRPC(drpc, testSpout));

        // You can use FeederBatchSpout to feed know values to the topology. Very useful for tests.
        testSpout.feed(fakeTweets.getNextTweetTuples("ted"));
        testSpout.feed(fakeTweets.getNextTweetTuples("ted"));
        testSpout.feed(fakeTweets.getNextTweetTuples("mary"));
        testSpout.feed(fakeTweets.getNextTweetTuples("jason"));

        // This is how you make DRPC calls. First argument must match the function name
        System.out.println(drpc.execute("ping", "ping pang pong"));
        System.out.println(drpc.execute("count", "america america ace ace ace item"));
        System.out.println(drpc.execute("count_per_actor", "ted"));
        System.out.println(drpc.execute("count_per_actors", "ted mary pere jason"));

        // You can use a client library to make calls remotely
//        DRPCClient client = new DRPCClient("drpc.server.location", 3772);
//        System.out.println(client.execute("ping", "ping pang pong"));
    }



    private static StormTopology basicStateAndDRPC(LocalDRPC drpc, FeederBatchSpout spout) throws IOException {
        TridentTopology topology = new TridentTopology();

        // persistentAggregate persists the result of aggregation into data stores,
        // which you can use from other applications.
        // You can also use it in other topologies by using the TridentState object returned.
        //
        // The state is commonly backed by a data store like memcache, cassandra etc.
        // Here we are simply using a hash map
        TridentState countState =
                topology
                        .newStream("spout", spout)
                        .groupBy(new Fields("actor"))
                        .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));

        // There are a few ready-made state libraries that you can use
        // Below is an example to use memcached
//        List<InetSocketAddress> memcachedServerLocations = ImmutableList.of(new InetSocketAddress("some.memcached.server",12000));
//        TridentState countStateMemcached =
//                topology
//                        .newStream("spout", spout)
//                        .groupBy(new Fields("actor"))
//                        .persistentAggregate(MemcachedState.transactional(memcachedServerLocations), new Count(), new Fields("count"));



        // DRPC stands for Distributed Remote Procedure Call
        // You can issue calls using the DRPC client library
        // A DRPC call takes two Strings, function name and function arguments
        //
        // In order to call the DRPC defined below, you'd use "count_per_actor" as the function name
        // The function arguments will be available as "args"
        topology
                .newDRPCStream("ping", drpc)
                .each(new Fields("args"), new Split(" "), new Fields("reply"))
                .each(new Fields("reply"), new RegexFilter("ping"))
                .project(new Fields("reply"));

        // You can apply usual processing primitives to DRPC streams as well
        topology
                .newDRPCStream("count", drpc)
                .each(new Fields("args"), new Split(" "), new Fields("split"))
                .each(new Fields("split"), new RegexFilter("a.*"))
                .groupBy(new Fields("split"))
                .aggregate(new Count(), new Fields("count"));


        // More usefully, you can query the state you created earlier
        topology
                .newDRPCStream("count_per_actor", drpc)
                .stateQuery(countState, new Fields("args"), new MapGet(), new Fields("count"));

        // Here is a more complex example
        topology
                .newDRPCStream("count_per_actors", drpc)
                .each(new Fields("args"), new Split(" "), new Fields("actor"))
                .groupBy(new Fields("actor"))
                .stateQuery(countState, new Fields("actor"), new MapGet(), new Fields("individual_count"))
                .each(new Fields("individual_count"), new FilterNull())
                .aggregate(new Fields("individual_count"), new Sum(), new Fields("count"));

        // For how to call DRPC calls, go back to the main method

        return topology.build();
    }



}
