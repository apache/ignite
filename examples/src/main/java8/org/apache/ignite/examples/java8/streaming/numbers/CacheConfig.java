package org.apache.ignite.examples.java8.streaming.numbers;

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;

import javax.cache.configuration.*;
import javax.cache.expiry.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * Created by Dmitriy on 3/18/15.
 */
public class CacheConfig {
    /** Cache name. */
    public static final String STREAM_NAME = "randomNumbers";

    /**
     * Configure streaming cache.
     */
    public static CacheConfiguration<Integer, Long> configure() {
        CacheConfiguration<Integer, Long> cfg = new CacheConfiguration<>();

        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setName(STREAM_NAME);
        cfg.setIndexedTypes(Integer.class, Long.class);

        // Sliding window of 1 seconds.
        cfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new CreatedExpiryPolicy(new Duration(SECONDS, 1))));

        return cfg;
    }
}
