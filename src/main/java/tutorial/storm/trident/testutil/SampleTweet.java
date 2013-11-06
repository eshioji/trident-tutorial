package tutorial.storm.trident.testutil;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.json.UTF8JsonGenerator;
import com.fasterxml.jackson.core.json.WriterBasedJsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.io.CharStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class SampleTweet {
    private static final Logger log = LoggerFactory.getLogger(SampleTweet.class);

    private final Iterator<String> sampleTweet;

    public SampleTweet() throws IOException {
        ObjectMapper om = new ObjectMapper();
        JsonFactory factory = new JsonFactory();
        ImmutableList.Builder<String> b = ImmutableList.builder();

        InputStreamReader reader = new InputStreamReader(this.getClass().getResourceAsStream("sample_tweet.json"));
        try {
            String tweetArray = CharStreams.toString(reader);
            ArrayNode parsed = (ArrayNode)om.readTree(tweetArray);
            for (JsonNode tweet : parsed) {
                StringWriter sw = new StringWriter();
                om.writeTree(factory.createGenerator(sw), tweet);
                b.add(sw.toString());
            }
            sampleTweet = Iterators.cycle(b.build());
        } finally {
            reader.close();
        }
    }

    public String sampleTweet(){
        return sampleTweet.next();
    }
}
