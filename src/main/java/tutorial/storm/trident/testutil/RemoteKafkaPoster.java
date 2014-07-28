package tutorial.storm.trident.testutil;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class RemoteKafkaPoster {
    private static final Logger log = LoggerFactory.getLogger(RemoteKafkaPoster.class);

    private final MetricRegistry metricRegistry;
    private final Producer<String, String> producer;

    public RemoteKafkaPoster(MetricRegistry metricRegistry, String kafkaHost, int kafkaPort) {
        this.metricRegistry = metricRegistry;
        String broker = kafkaHost + ":" + kafkaPort;
        Properties props = new Properties();
        props.put("metadata.broker.list", broker);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }



    public void post(String topic, String message) {
        Timer.Context t = metricRegistry.timer(this.getClass().getSimpleName()+".kafka-post." + topic).time();
        KeyedMessage<String,String> msg = new KeyedMessage<String, String>(topic, message);
        producer.send(msg);
        t.stop();
    }

}
