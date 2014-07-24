package tutorial.storm.trident;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.TException;

/**
 * @author Enno Shioji (eshioji@gmail.com)
 */
public class DrpcClientExampleForSkeleton {
    public static void main(String[] args) throws TException, DRPCExecutionException {
        DRPCClient cl = new DRPCClient("localhost",3772, 3000);
        System.out.println(cl.execute("ping", "a"));

    }
}
