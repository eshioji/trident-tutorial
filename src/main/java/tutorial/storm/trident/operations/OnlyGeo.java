package tutorial.storm.trident.operations;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import twitter4j.Status;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class OnlyGeo extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tuple) {
        Status status = (Status) tuple.get(0);
        return !(null == status.getPlace() || null == status.getPlace().getCountryCode());
    }
}
