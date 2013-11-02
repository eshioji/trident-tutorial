package tutorial.storm.trident.testutil;

import com.google.common.io.CharStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class SampleTweet {
    private static final Logger log = LoggerFactory.getLogger(SampleTweet.class);

    private final String sampleTweet;

    public SampleTweet() throws IOException {
        InputStreamReader reader = new InputStreamReader(this.getClass().getResourceAsStream("sample_tweet.json"));
        try {
            sampleTweet = CharStreams.toString(reader);
        } finally {
            reader.close();
        }
    }

    public String sampleTweet(){
        return sampleTweet;
    }
}
