package tutorial.storm.trident;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.TException;

/**
 * @author Enno Shioji (eshioji@gmail.com)
 */
public class DrpcTestClient {
    public static void main(String[] args) throws TException, DRPCExecutionException {
        DRPCClient cl = new DRPCClient("localhost",3772, 3000);
        if (args.length != 2){
            System.err.println("<functionName> <arguments>");
        }else{
            String func = args[0];
            String argument = args[1];
            System.out.println(cl.execute(func, argument));
        }

    }
}
