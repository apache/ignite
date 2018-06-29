package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;

import org.apache.ignite.cache.query.SqlFieldsQuery;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.query.h2.twostep.GridMapQueryExecutor;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;

public class Main {

    public static final String CACHE = "cache";

    public static void main(String[] args) throws InterruptedException {
        Ignition.start(getConfig().setIgniteInstanceName("node1"));
        Ignition.start(getConfig().setIgniteInstanceName("node2"));

        Random random = new Random(1);
        Ignite client = Ignition.start(getConfig().setClientMode(true));

        int ctr = 0;

        while (true) {
            String cacheName = CACHE + ctr++;

            System.out.println("CACHE NAME: " + cacheName);

            IgniteCache<Long, Value> cache = client.getOrCreateCache(new CacheConfiguration<Long, Value>(cacheName)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(1)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setIndexedTypes(Long.class, Value.class));

            System.out.println("Populating cache...");
            cache.putAll(LongStream.range(610026643276160000L, 610026643276170000L).boxed()
                .collect(Collectors.toMap(Function.identity(),
                    t -> Value.of(new byte[(random.nextInt(16)) * 1000]))));

            if (ctr == 5) {
                GridMapQueryExecutor.DEBUG = true;
                IndexingQueryCacheFilter.DEBUG = true;
            }

            for (int i = 0; i < 10; i++) {
                System.out.print(i == 9 ? "\n" : ".");
                long start = 610026643276160000L;
                long end = start + random.nextInt(10);

                int expectedResultCount = (int) (end - start + 1);

                String sql = String.format(
                    "SELECT _KEY " +
                        "FROM %s " +
                        "WHERE _KEY >= %d AND _KEY <= %d", Value.class.getSimpleName().toLowerCase(),
                    start, end);

                Set<Long> keySet = new HashSet<>();
                for (long l = start; l < end + 1; l++)
                    keySet.add(l);
                int size = cache.getAll(keySet).entrySet().size();

                List<Long> resultKeys;
                try(FieldsQueryCursor<List<?>> results = cache.query(new SqlFieldsQuery(sql))) {
                    resultKeys = new ArrayList<>();
                    results.forEach(objects -> resultKeys.add((Long)objects.get(0)));
                    Collections.sort(resultKeys);
                }

                if (resultKeys.size() != expectedResultCount) {
                    System.out.printf("Expected %d results but got back %d results " +
                        "(query range %d to %d), cache.getAll returned %d entries.%n", expectedResultCount, resultKeys.size(), start, end, size);
                    System.out.printf("Query results: %s%n", resultKeys);

                    System.exit(-1);
                }

                Thread.sleep(100);
            }
            cache.destroy();
        }
    }

    /**
     * Ignite configuration for this example.
     *
     * @return Ignite configuration
     */
    static IgniteConfiguration getConfig() {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setConsistentId(cfg.getIgniteInstanceName());
        cfg.setIncludeEventTypes(EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);

        TcpDiscoverySpi discovery = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();
        finder.setAddresses(Collections.singletonList("127.0.0.1:47500..47501"));
        discovery.setIpFinder(finder);
        cfg.setDiscoverySpi(discovery);

        DataStorageConfiguration storageConfiguration = new DataStorageConfiguration();
        storageConfiguration.setPageSize(1024 * 8);
        cfg.setDataStorageConfiguration(storageConfiguration);
        return cfg;
    }

    /**
     * Class containing value to be placed into the cache.
     */
    public static class Value implements Serializable {
        final byte[] data;

        public Value(final byte[] data) {
            this.data = data;
        }

        public static Value of(final byte[] bytes) {
            return new Value(bytes);
        }
    }
}
