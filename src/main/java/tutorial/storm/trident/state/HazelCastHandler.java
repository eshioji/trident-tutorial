package tutorial.storm.trident.state;

import com.hazelcast.core.Hazelcast;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class HazelCastHandler<T> implements Serializable {

    private transient Map<List<Object>, T> state;

    public Map<List<Object>, T> getState() {
        if (state == null) {
            state = Hazelcast.newHazelcastInstance().getMap("state");
        }
        return state;
    }
}