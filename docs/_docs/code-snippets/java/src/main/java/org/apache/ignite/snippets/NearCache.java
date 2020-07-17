package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicyFactory;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;

public class NearCache {

    public static void main(String[] args) {
        Ignite ignite = Ignition.start();

        // tag::nearCacheConfiguration[]
        // Create a near-cache configuration for "myCache".
        NearCacheConfiguration<Integer, Integer> nearCfg = new NearCacheConfiguration<>();

        // Use LRU eviction policy to automatically evict entries
        // from near-cache whenever it reaches 100_000 entries
        nearCfg.setNearEvictionPolicyFactory(new LruEvictionPolicyFactory<>(100_000));

        CacheConfiguration<Integer, Integer> cacheCfg = new CacheConfiguration<Integer, Integer>("myCache");

        cacheCfg.setNearConfiguration(nearCfg);

        // Create a distributed cache on server nodes 
        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheCfg);
        // end::nearCacheConfiguration[]

    }

    public void createDynamically() {

        Ignition.setClientMode(true);

        Ignite ignite = Ignition.start();

        // tag::createNearCacheDynamically[]
        // Create a near-cache configuration
        NearCacheConfiguration<Integer, String> nearCfg = new NearCacheConfiguration<>();

        // Use LRU eviction policy to automatically evict entries
        // from near-cache, whenever it reaches 100_000 in size.
        nearCfg.setNearEvictionPolicyFactory(new LruEvictionPolicyFactory<>(100_000));

        // get the cache named "myCache" and create a near cache for it
        IgniteCache<Integer, String> cache = ignite.getOrCreateNearCache("myCache", nearCfg);

        String value = cache.get(1);
        // end::createNearCacheDynamically[]
    }
}
