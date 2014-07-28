package tutorial.storm.trident.testutil;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;

/**
 * @author Enno Shioji (eshioji@gmail.com)
 */
public class TestUtils {
    public static TransactionalTridentKafkaSpout testTweetSpout(BrokerHosts hosts) {
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(hosts, "test", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new TransactionalTridentKafkaSpout(kafkaConfig);
    }

}
