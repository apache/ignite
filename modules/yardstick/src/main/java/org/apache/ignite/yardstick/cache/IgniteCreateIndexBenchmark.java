package org.apache.ignite.yardstick.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.yardstick.cache.model.Person8NotIndexed;
import org.jsr166.LongAdder8;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 * Benchmark for a dynamic creating indexes time measurement.
 * In <code>setUp()</code> method cache populates with unindexed {@link Person8NotIndexed} items.
 * Number of preloaded items is taken from the <code>--preloadAmount</code> argument value.
 * <code>test()</code> method runs only once and only by the one thread (other threads are ignored).
 * It creates dynamic index over the cache with SQL statement and measures time of it's creation.
 * Results will be available in logs after the benchmark's end.
 */
public class IgniteCreateIndexBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** Cache name. */
    private static final String CACHE_NAME = "Person8NotIndexed";

    /** Cache entries quantity. */
    // TODO: Remove local variable.
    private int quantity;

    /** Number of threads for benchmark. */
    // TODO: Remove local variable.
    private int threads;

    /** Cache configuration */
    private CacheConfiguration<Integer, Object> personCacheCfg = getCfg();

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        // TODO: Remove atomics
        IgniteAtomicLong started = ignite().atomicLong("started", 0, true);

        long startedVal = started.getAndIncrement();

        if (startedVal == 0) { // Only the first thread creates indexes
            BenchmarkUtils.println("IgniteCreateIndexBenchmark started creating indexes over the cache [size=" +
                cache().size() + ']');

            // TODO: Index on a single column.
            SqlFieldsQuery qry = new SqlFieldsQuery("CREATE INDEX idx_person8 ON " + CACHE_NAME +
                " (val2, val3, val4, val5, val6, val7, val8)");

            final long start = System.currentTimeMillis();

            cache().query(qry).getAll();

            final long stop = System.currentTimeMillis();

            BenchmarkUtils.println("IgniteCreateIndexBenchmark =========================================");
            BenchmarkUtils.println("IgniteCreateIndexBenchmark created index in " + (stop - start) + " ms");
            BenchmarkUtils.println("IgniteCreateIndexBenchmark =========================================");
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().getOrCreateCache(personCacheCfg);
    }

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        // TODO: Throw exception if more than 1 thread.
        super.setUp(cfg);

        quantity = args.preloadAmount();

        if (quantity < 1)
            throw new IllegalArgumentException("Invalid number of entries: " + quantity);

        BenchmarkUtils.println("Preload entries [quantity=" + quantity + ']');

        // TODO: Threads == CPU count

        threads = cfg.threads();

        if (threads < 1)
            throw new IllegalArgumentException("Invalid number of threads: " + threads);

        BenchmarkUtils.println("Preloaders thread pool size [threads=" + threads + ']');

        BenchmarkUtils.println("Local node: " + ignite().cluster().localNode());

        BenchmarkUtils.println("Cluster nodes: " + ignite().cluster().nodes());

        // TODO: Not needed
        IgniteCompute compute = ignite().compute();

        // Run cache preloading on the several nodes if any
        compute.broadcast(new IgniteRunnable() {
            @Override public void run() {
                try (IgniteDataStreamer<Integer, Person8NotIndexed> streamer = ignite().dataStreamer(CACHE_NAME)) {
                    ExecutorService executor = Executors.newFixedThreadPool(threads);

                    // TODO: Not needed
                    final IgniteAtomicSequence seq = ignite().atomicSequence(CACHE_NAME, 0, true);

                    List<Future<Void>> futs = new ArrayList<>();

                    final LongAdder8 cntr = new LongAdder8();

                    final long start = System.currentTimeMillis();

                    for (int i = 0; i < threads; i++) {
                        futs.add(executor.submit(new Callable<Void>() {
                            @Override public Void call() throws Exception {
                                int val = (int)seq.getAndIncrement();

                                while (val < quantity) {
                                    Person8NotIndexed person = createFromValue(val);

                                    // TODO: Use manual batching (map of 1000 elements)
                                    streamer.addData(val, person);

                                    cntr.increment();

                                    val = (int)seq.getAndIncrement();
                                }
                                return null;
                            }
                        }));
                    }

                    for (Future<Void> fut : futs) {
                        try {
                            fut.get();
                        }
                        catch (Exception e) {
                            BenchmarkUtils.println("Exception on node [id=" + ignite().cluster().localNode().id() +
                                ", exc=" + e + ']');
                        }
                    }

                    streamer.flush();

                    final long stop = System.currentTimeMillis();

                    BenchmarkUtils.println("Node [id=" + ignite().cluster().localNode().id() +
                        "] populated cache with [cntr=" + cntr.sum() + "]  entries in " + (stop - start) + "ms");
                }
            }
        });

        BenchmarkUtils.println("Total cache size after preloading [size=" + cache().size() + ']');
    }

    /**
     * Creates benchmark configuration
     *
     * @return benchmark configuration
     */
    // TODO: getConfiguration
    private CacheConfiguration<Integer, Object> getCfg() {
        QueryEntity entity = new QueryEntity();

        entity.setKeyType(Integer.class.getName());
        entity.setValueType(Person8NotIndexed.class.getName());

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();

        fields.put("val1", Integer.class.getName());
        fields.put("val2", Integer.class.getName());
        fields.put("val3", Integer.class.getName());
        fields.put("val4", Integer.class.getName());
        fields.put("val5", Integer.class.getName());
        fields.put("val6", Integer.class.getName());
        fields.put("val7", Integer.class.getName());
        fields.put("val8", Integer.class.getName());

        entity.setFields(fields);

        entity.setKeyFieldName("val1");

        personCacheCfg = new CacheConfiguration<>(CACHE_NAME);

        personCacheCfg.setSqlSchema("PUBLIC").setQueryEntities(Arrays.asList(entity));

        return personCacheCfg;
    }

    /**
     * Returns initialized Person8 with duplicated val2-val8 fields.
     *
     * @param val primary key
     * @return initialized Person8
     */
    private Person8NotIndexed createFromValue(int val) {
        Person8NotIndexed person = new Person8NotIndexed();

        person.setVal1(val);
        person.setVal2(val % 5);
        person.setVal3(val % 11);
        person.setVal4(val % 51);
        person.setVal5(val % 101);
        person.setVal6(val % 503);
        person.setVal7(val % 1001);
        person.setVal8(val % 5003);

        return person;
    }
}
