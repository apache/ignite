package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.junit.jupiter.api.Test;

public class BasicCacheOperations {

    @Test
    void getCacheInstanceExample() {
        Ignition.start();
        Ignition.ignite().createCache("myCache");
        // tag::getCache[]
        Ignite ignite = Ignition.ignite();

        // Obtain an instance of the cache named "myCache".
        // Note that different caches may have different generics.
        IgniteCache<Integer, String> cache = ignite.cache("myCache");
        // end::getCache[]
        Ignition.ignite().close();
    }

    @Test
    void createCacheExample() {
        Ignition.start();
        // tag::createCache[]
        Ignite ignite = Ignition.ignite();

        CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>();

        cfg.setName("myNewCache");
        cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        // Create a cache with the given name if it does not exist.
        IgniteCache<Integer, String> cache = ignite.getOrCreateCache(cfg);
        // end::createCache[]
        Ignition.ignite().close();
    }

    @Test
    void destroyCacheExaple() {
        Ignition.start();
        Ignition.ignite().createCache("myCache");
        // tag::destroyCache[]
        Ignite ignite = Ignition.ignite();

        IgniteCache<Long, String> cache = ignite.cache("myCache");

        cache.destroy();
        // end::destroyCache[]
        Ignition.ignite().close();
    }

    @Test
    void atomicOperationsExample() {
        try (Ignite ignite = Ignition.start()) {
            ignite.createCache("myCache");
            // tag::atomic1[]
            IgniteCache<Integer, String> cache = ignite.cache("myCache");

            // Store keys in the cache (the values will end up on different cache nodes).
            for (int i = 0; i < 10; i++)
                cache.put(i, Integer.toString(i));

            for (int i = 0; i < 10; i++)
                System.out.println("Got [key=" + i + ", val=" + cache.get(i) + ']');
            // end::atomic1[]
            // tag::atomic2[]
            // Put-if-absent which returns previous value.
            String oldVal = cache.getAndPutIfAbsent(11, "Hello");

            // Put-if-absent which returns boolean success flag.
            boolean success = cache.putIfAbsent(22, "World");

            // Replace-if-exists operation (opposite of getAndPutIfAbsent), returns previous
            // value.
            oldVal = cache.getAndReplace(11, "New value");

            // Replace-if-exists operation (opposite of putIfAbsent), returns boolean
            // success flag.
            success = cache.replace(22, "Other new value");

            // Replace-if-matches operation.
            success = cache.replace(22, "Other new value", "Yet-another-new-value");

            // Remove-if-matches operation.
            success = cache.remove(11, "Hello");
            // end::atomic2[]
        }
    }

    @Test
    void asyncExecutionExample() {
        try (Ignite ignite = Ignition.start()) {
            // tag::async[]
            IgniteCompute compute = ignite.compute();

            // Execute a closure asynchronously.
            IgniteFuture<String> fut = compute.callAsync(() -> "Hello World");

            // Listen for completion and print out the result.
            fut.listen(f -> System.out.println("Job result: " + f.get()));
            // end::async[]
        }
    }
}
