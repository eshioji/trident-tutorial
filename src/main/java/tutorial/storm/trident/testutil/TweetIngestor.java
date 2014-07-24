package tutorial.storm.trident.testutil;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractService;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.json.DataObjectFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;


/**
 * This is a test utility class to create a local kafka broker that streams tweets
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class TweetIngestor extends AbstractService {
    private static final Logger log = LoggerFactory.getLogger(TweetIngestor.class);
    private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
    static{
        Slf4jReporter.forRegistry(METRIC_REGISTRY).outputTo(log).build().start(15, TimeUnit.SECONDS);

    }
    private final KafkaLocalBroker kafkaLocalBroker;
    private final TwitterStream stream;
    private final Producer<String, String> producer;

    /**
     *
     * @param kafkaDataDir where Kafka stores its data
     * @param kafkaTopic the topic name on which to publish the tweets
     * @param kafkaPort the port for Kafka clients to connect
     */
    public TweetIngestor(String kafkaDataDir,String kafkaTopic, int kafkaPort) {
        this.kafkaLocalBroker = new KafkaLocalBroker(kafkaDataDir,kafkaTopic,kafkaPort);
        TwitterStreamFactory fact = new TwitterStreamFactory();
        checkState(fact.getInstance().getConfiguration().isJSONStoreEnabled(), "Twitter4j JSON store is disabled. You must enabled it in the twitter4j.properties file!");
        stream = fact.getInstance();
        Properties props = new Properties();
        props.put("broker.list", kafkaLocalBroker.localhostBroker);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }


    @Override
    protected void doStart() {
        new Thread() {
            @Override
            public void run() {
                try {
                    kafkaLocalBroker.startAndWait();
                    StatusAdapter listener = new StatusAdapter() {
                        @Override
                        public void onStatus(Status status) {
                            Timer.Context t = METRIC_REGISTRY.timer("tweet-ingestion").time();
                            String rawJson = DataObjectFactory.getRawJSON(status);
                            if(StringUtils.isEmpty(rawJson)){
                                return;
                            }
                            ProducerData<String, String> data = new ProducerData<String, String>(kafkaLocalBroker.topic, rawJson);
                            producer.send(data);
                            t.stop();
                        }

                        @Override
                        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                            return;
                        }

                        @Override
                        public void onStallWarning(StallWarning warning) {
                            log.warn("Received:" + warning);
                        }
                    };

                    stream.addListener(listener);
                    stream.sample();
                    notifyStarted();
                } catch (Throwable e) {
                    notifyFailed(e);
                    throw Throwables.propagate(e);
                }
            }
        }.start();
    }

    @Override
    protected void doStop() {
        new Thread() {
            @Override
            public void run() {
                try {
                    stream.shutdown();
                    producer.close();
                    kafkaLocalBroker.stopAndWait();
                    notifyStopped();
                } catch (Throwable e) {
                    notifyFailed(e);
                    throw Throwables.propagate(e);
                }

            }
        }.start();
    }

    public static void main(String[] args) {
        checkArgument(args.length > 0 && args.length < 3, "Incorrect arguments. Usage: kafkaLogDir [port]");
        String logdir = args[0];
        int port = args.length == 2 ? Integer.valueOf(args[1]) : 12000;
        TweetIngestor ingestor = new TweetIngestor(logdir,"test",port);
        checkState(State.RUNNING == ingestor.startAndWait());
        log.info("Tweet ingestor started");

    }

}
