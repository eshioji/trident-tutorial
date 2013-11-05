package tutorial.storm.trident.testutil;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.jvm.hotspot.jdi.ShortValueImpl;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class Content {
    private static final Logger log = LoggerFactory.getLogger(Content.class);

    private long tweetId;
    private long userId;
    private long createdAtMs;

    private String contentName;
    private String contentType;

    public Content(long tweetId, long userId, long createdAtMs) {
        this.tweetId = tweetId;
        this.userId = userId;
        this.createdAtMs = createdAtMs;
    }

    public String getContentId(){
        HashFunction md5 = Hashing.md5();
        return md5.hashString(contentType + contentName).toString();
    }

    public String getContentName() {
        return contentName;
    }

    public void setContentName(String contentName) {
        this.contentName = contentName;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    @Override
    public String toString() {
        return "Content{" +
                "contentType='" + contentType + '\'' +
                ", contentName='" + contentName + '\'' +
                '}';
    }

    public String getContentType() {
        return contentType;
    }
}
