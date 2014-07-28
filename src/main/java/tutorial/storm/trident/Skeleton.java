package tutorial.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FirstN;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.TupleCollectionGet;
import storm.trident.testing.FeederBatchSpout;
import storm.trident.testing.MemoryMapState;
import tutorial.storm.trident.operations.*;
import tutorial.storm.trident.testutil.SampleTweet;
import tutorial.storm.trident.testutil.TweetIngestor;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;


/**
* @author Enno Shioji (enno.shioji@peerindex.com)
*/
public class Skeleton {
    private static final Logger log = LoggerFactory.getLogger(Skeleton.class);
    public static StormTopology buildTopology(TransactionalTridentKafkaSpout spout) throws IOException {
        TridentTopology topology = new TridentTopology();
        topology
                .newStream("tweets", spout)
                .each(new Fields("str"), new Print())
        ;

        topology
                .newDRPCStream("ping");

        return topology.build();
    }


    public static void main(String[] args) throws Exception {
        Config conf = new Config();


        if (args.length == 2) {
            // Ready & submit the topology
            String name = args[0];
            BrokerHosts hosts = new ZkHosts(args[1]);
            TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(hosts, "test", "storm");
            kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(kafkaConfig);

            StormSubmitter.submitTopology(name, conf, buildTopology(kafkaSpout));

        }else{
            System.err.println("<topologyName> <zookeeperHost>");
        }

    }

}
