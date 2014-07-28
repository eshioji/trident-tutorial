package tutorial.storm.trident.state;


import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import storm.trident.state.ITupleCollection;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.IBackingMap;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;


public class HazelCastState<T> implements IBackingMap<TransactionalValue<T>> {

    private HazelCastHandler handler;


    public HazelCastState(HazelCastHandler handler) {
        this.handler = handler;
    }

    public void addKeyValue(String key, T value) {
        Map<String, T> state = handler.getState();
        state.put(key, value);
    }

    @Override
    public String toString() {
        return handler.getState().toString();
    }


    @Override
    public void multiPut(List<List<Object>> keys, List<TransactionalValue<T>> vals) {
        for (int i = 0; i < keys.size(); i++) {
            String key = getKey(keys.get(i));
            T value = vals.get(i).getVal();
            addKeyValue(key, value);
        }
    }


    public List<TransactionalValue<T>> multiGet(List<List<Object>> keys) {
        List<TransactionalValue<T>> result = new ArrayList<TransactionalValue<T>>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            String key = getKey(keys.get(i));
            result.add(new TransactionalValue<T>(0L, (T)(handler.getState().get(key))));
        }
        return result;
    }

    private static final Joiner JOINER = Joiner.on("_");
    private String getKey(List<Object> keys) {
        checkState(Iterables.all(keys, new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                return input instanceof String;
            }
        }), "All element must be String!");

        return JOINER.join(keys);
    }

}