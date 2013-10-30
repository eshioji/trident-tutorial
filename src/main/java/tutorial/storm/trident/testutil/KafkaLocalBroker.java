package tutorial.storm.trident.testutil;

import com.google.common.util.concurrent.AbstractService;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

/**
 * Provides a local kafka broker for testing purposes
 */
public class KafkaLocalBroker extends AbstractService {
    private static final Logger log = LoggerFactory.getLogger(KafkaLocalBroker.class);

    private static final int BROKER_ID = 1;

    public final String logDir;
    public final String localhostBroker;
    public final String topic;

    public final KafkaConfig kafkaConfig;
    public final KafkaServer kafkaServer;


    @Override
    protected void doStart() {
        new Thread(){
            @Override
            public void run() {
                //delete old Kafka topic files
                File logDir = new File(KafkaLocalBroker.this.logDir);
                if (logDir.exists()) {
                    deleteDir(logDir);
                }

                kafkaServer.startup();
                log.info("Embedded kafka startup requested:" + kafkaConfig);
                notifyStarted();
            }
        }.start();
    }

    @Override
    protected void doStop() {
        new Thread(){
            @Override
            public void run() {
                kafkaServer.shutdown();
                log.info("Embedded kafka shutdown requested");
                notifyStopped();
            }
        }.start();

    }


    public KafkaLocalBroker(String logDir, String topic, int port) {
        kafkaConfig = new KafkaConfig(createProperties(logDir, port, BROKER_ID));
        kafkaServer = new KafkaServer(kafkaConfig);
        this.logDir = logDir;
        this.localhostBroker = "0:localhost:"+port;
        this.topic = topic;
    }

    private static Properties createProperties(String logDir, int port, int brokerId) {
        Properties properties = new Properties();
        properties.put("port", port + "");
        properties.put("brokerid", brokerId + "");
        properties.put("log.dir", logDir);
        properties.put("enable.zookeeper", "false");
        return properties;
    }

    private static void deleteDir(File folder) {
        File[] files = folder.listFiles();
        if (files != null) { //some JVMs return null for empty dirs
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDir(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }
}