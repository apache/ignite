package events;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

public class StartAudit {
    public static void main(String[] args) {
        Ignition.setClientMode(true);
        try(Ignite ignite = Ignition.start(StartGrid.CFG)){

            IgniteCache<String, String> cache = ignite.getOrCreateCache(new CacheConfiguration<String, String>()
                .setName("UserCache")
                .setCacheMode(CacheMode.REPLICATED)
            );

            ignite.compute()
                .run(new AuditRun(cache));
        }
    }
}
