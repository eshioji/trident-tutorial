package tutorial.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.spout.IBatchSpout;
import storm.trident.testing.MemoryMapState;
import tutorial.storm.trident.operations.DebugFilter;
import tutorial.storm.trident.operations.RegexFilter;
import tutorial.storm.trident.operations.ToUpperCase;
import tutorial.storm.trident.testutil.FakeTweetsBatchSpout;

import java.io.IOException;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class Part01_BasicPrimitives {
    private static final Logger log = LoggerFactory.getLogger(Part01_BasicPrimitives.class);

    public static void main(String[] args) throws Exception {
        // Storm can be run locally for testing purposes
        Config conf = new Config();
//        conf.put(Config.TOPOLOGY_DEBUG,true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("basic_primitives", conf, basicPrimitives(new FakeTweetsBatchSpout()));
        Thread.sleep(30000);
        cluster.shutdown();
        
    }

    public static StormTopology basicPrimitives(IBatchSpout spout) throws IOException {

        // A topology is a set of streams.
        // A stream is a DAG of Spouts and Bolts.
        // (In Storm there are Spouts (data producers) and Bolts (data processors).
        // Spouts create Tuples and Bolts manipulate then and possibly emit new ones.)

        // But in Trident we operate at a higher level.
        // Bolts are created and connected automatically out of higher-level constructs.
        // Also, Spouts are "batched".
        TridentTopology topology = new TridentTopology();

        // The "each" primitive allows us to apply either filters or functions to the stream
        // We always have to select the input fields.
        topology
                .newStream("filter", spout)
                .each(new Fields("actor"), new RegexFilter("pere"))
                .each(new Fields("text", "actor"), new DebugFilter());

        // Functions describe their output fields, which are always appended to the input fields.
        // As you see, Each operations can be chained.
        topology
                .newStream("function", spout)
                .each(new Fields("text"), new ToUpperCase(), new Fields("uppercased_text"))
                .each(new Fields("text", "uppercased_text"), new DebugFilter());

        // You can prune unnecessary fields using "project"
        topology
                .newStream("projection", spout)
                .each(new Fields("text"), new ToUpperCase(), new Fields("uppercased_text"))
                .project(new Fields("uppercased_text"))
                .each(new Fields("uppercased_text"), new DebugFilter());

        // Stream can be parallelized with "parallelismHint"
        // Parallelism hint is applied downwards until a partitioning operation (we will see this later).
        // This topology creates 5 spouts and 5 bolts:
        // Let's debug that with TridentOperationContext.partitionIndex !
        topology
                .newStream("parallel", spout)
                .each(new Fields("actor"), new RegexFilter("pere"))
                .parallelismHint(5)
                .each(new Fields("text", "actor"), new DebugFilter());

        // You can perform aggregations by grouping the stream and then applying an aggregation
        // Note how each actor appears more than once. We are aggregating inside small batches (aka micro batches)
        // This is useful for pre-processing before storing the result to databases
        topology
                .newStream("aggregation", spout)
                .groupBy(new Fields("actor"))
                .aggregate(new Count(),new Fields("count"))
                .each(new Fields("actor", "count"),new DebugFilter())
        ;

        // In order ot aggregate across batches, we need persistentAggregate.
        // This example is incrementing a count in the DB, using the result of these micro batch aggregations
        // (here we are simply using a hash map for the "database")
        topology
                .newStream("aggregation", spout)
                .groupBy(new Fields("actor"))
                .persistentAggregate(new MemoryMapState.Factory(),new Count(),new Fields("count"))
        ;

        return topology.build();
    }

}
