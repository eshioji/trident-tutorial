package tutorial.storm.trident.testutil;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.RateLimiter;
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

    private final RateLimiter rateLimiter;
    private final TwitterStream stream;
    private final RemoteKafkaPoster poster;
    private final String kafkaTopic;
    private final String track;

    /**
     *
     * @param kafkaHost the host
     * @param kafkaTopic the topic name on which to publish the tweets
     * @param kafkaPort the port for Kafka clients to connect
     */
    public TweetIngestor(String kafkaHost,String kafkaTopic, int kafkaPort, String track, double tweetRate) {
        TwitterStreamFactory fact = new TwitterStreamFactory();
        checkState(fact.getInstance().getConfiguration().isJSONStoreEnabled(), "Twitter4j JSON store is disabled. You must enabled it in the twitter4j.properties file!");
        stream = fact.getInstance();
        poster = new RemoteKafkaPoster(METRIC_REGISTRY, kafkaHost, kafkaPort);
        this.kafkaTopic = kafkaTopic;
        this.rateLimiter = RateLimiter.create(tweetRate);
        this.track = track;
    }


    @Override
    protected void doStart() {
        new Thread() {
            @Override
            public void run() {
                try {
                    StatusAdapter listener = new StatusAdapter() {
                        @Override
                        public void onStatus(Status status) {
                            if(rateLimiter.tryAcquire()) {
                                Timer.Context t = METRIC_REGISTRY.timer("tweet-ingestion").time();
                                String rawJson = DataObjectFactory.getRawJSON(status);
                                if (StringUtils.isEmpty(rawJson)) {
                                    return;
                                }
                                poster.post(kafkaTopic, rawJson);
                                t.stop();
                            }else{
                                // Throwing away tweets!
                            }
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
                    FilterQuery fq = new FilterQuery();
                    fq.track(new String[]{ track });
                    stream.filter(fq);
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
                    notifyStopped();
                } catch (Throwable e) {
                    notifyFailed(e);
                    throw Throwables.propagate(e);
                }

            }
        }.start();
    }

    public static void main(String[] args) {
        checkArgument(args.length == 4, "Incorrect arguments. Usage: kafkaHost port trackKeyword tweetRate");
        String kafkahost = args[0];
        int port = Integer.valueOf(args[1]);
        String track = args[2];
        double tweetRate = Double.valueOf(args[3]);

        TweetIngestor ingestor = new TweetIngestor(kafkahost, "test", port, track, tweetRate);
        checkState(State.RUNNING == ingestor.startAndWait());
        log.info("Tweet ingestor started");

    }

}
