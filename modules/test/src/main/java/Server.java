import javafx.util.Pair;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

import java.net.URL;

/**
 * Created by freem on 25.04.2017.
 */
public class Server {
    public static void main(String[] args) throws Exception {
        Ignition.setClientMode(false);
        URL resource = Server.class.getResource("example-ignite.xml");
        try (Ignite ignite = Ignition.start(resource)) {
            assert !ignite.configuration().isClientMode();
//                ignite.configuration().setPeerClassLoadingLocalClassPathExclude(StreamTransformer.class.getPackage().getName()+"*");
            IgniteCache<String, Pair<Integer, Integer>> cache = ignite.getOrCreateCache("test");
            cache.put("zero", new Pair<>(0, 0));
            assert new Pair<>(0, 0).equals(cache.get("zero"));
            while(true)
                Thread.sleep(10000L);
        }
    }
}
