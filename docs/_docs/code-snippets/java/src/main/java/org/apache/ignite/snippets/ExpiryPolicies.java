package org.apache.ignite.snippets;

import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.Test;

public class ExpiryPolicies {

    @Test
   void expiryPoliciesExample() {
        //tag::cfg[]
        //tag::eagerTtl[]
        CacheConfiguration<Integer, String> cfg = new CacheConfiguration<Integer, String>();
        cfg.setName("myCache");
        //end::cfg[]

        cfg.setEagerTtl(true);
        //end::eagerTtl[]
        //tag::cfg[]
        cfg.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.FIVE_MINUTES));
        //end::cfg[]

    }

    @Test
    void expiryPolicyForIndividualEntry() {

        IgniteConfiguration igniteCfg = new IgniteConfiguration();
        try (Ignite ignite = Ignition.start(igniteCfg)) {

            //tag::expiry2[]

            CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<Integer, String>("myCache");

            ignite.createCache(cacheCfg);

            IgniteCache cache = ignite.cache("myCache")
                    .withExpiryPolicy(new CreatedExpiryPolicy(new Duration(TimeUnit.MINUTES, 5)));

            // if the cache does not contain key 1, the entry will expire after 5 minutes
            cache.put(1, "first value");

            //end::expiry2[]
        }
    }
}
