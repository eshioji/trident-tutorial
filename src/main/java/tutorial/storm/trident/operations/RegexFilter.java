package tutorial.storm.trident.operations;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

import java.util.regex.Pattern;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class RegexFilter extends BaseFilter {
    private final Pattern pattern;

    public RegexFilter(String pattern) {
        this.pattern = Pattern.compile(pattern);
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        String string = tuple.getString(0);
        return pattern.matcher(string).matches();
    }
}
