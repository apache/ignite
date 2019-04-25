package org.sega.events;

//import java.time.LocalDateTime;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

public class AuditExample {
    public static void main(String[] args) {
        Ignition.setClientMode(true);
        /** Start ignite*/
        try(Ignite ignite = Ignition.start(StartGrid.CFG)) {

            /** Create caches */
            IgniteCache<String, String> cache = ignite.getOrCreateCache(new CacheConfiguration<String, String>()
                .setName("UserCache")
                .setCacheMode(CacheMode.REPLICATED)
            );

//            IgniteCache<LocalDateTime,String> auditCache = ignite.getOrCreateCache(new CacheConfiguration<LocalDateTime, String>()
//                .setName("AuditCache")
//                .setCacheMode(CacheMode.REPLICATED)
//            );

//            for (int i = 0; i < 20; i++)
//                cache.put("name" + i, new SecurityInfo());

            for (int i = 0; i < 10; i++)
                cache.put("name" + i, "value"+1);

//            for (int i = 0; i < 10; i++)
//                cache.remove("name" + i);

//            ignite.cluster().nodes().forEach(s->{
//                System.out.println(s.id());
//                UUID id =s.id();
//                if (id.toString().contains("2cc82e9e"))
//                    ignite.cluster().stopNodes(Arrays.asList(id));
//            });

            cache.forEach(e -> {
                System.out.println("key=" + e.getKey() + ", value=" + e.getValue());
            });

//            System.out.println("Audit");

//            auditCache.forEach(e->System.out.println("key=" + e.getKey() + ", value=" + e.getValue()));

            /** destroy caches*/
//            cache.destroy();
//            auditCache.destroy();
        }

    }
}
