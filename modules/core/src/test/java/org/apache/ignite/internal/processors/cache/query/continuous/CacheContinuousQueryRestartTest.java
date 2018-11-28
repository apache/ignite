package org.apache.ignite.internal.processors.cache.query.continuous;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import java.io.File;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.synchronizedSet;
import static javax.cache.configuration.FactoryBuilder.factoryOf;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_MMAP;

public class CacheContinuousQueryRestartTest extends GridCommonAbstractTest {

    private static final int GRID_CNT = 4;

    private static final int PAGE_SIZE = DataStorageConfiguration.DFLT_PAGE_SIZE;

    private static final int WAL_SEGMENT_SIZE = 1024 * PAGE_SIZE;

    private static final int ENTRIES_COUNT = 10_000;

    protected static final String CACHE_NAME = "test_cache";

    private boolean clientMode = false;

    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(clientMode);

        File discoveryFsPath = U.resolveWorkDirectory(U.defaultWorkDirectory(), TcpDiscoverySharedFsIpFinder.DFLT_PATH, false);
        DiscoverySpi dspi = new TcpDiscoverySpi().
                setIpFinder(new TcpDiscoverySharedFsIpFinder()
                        .setPath(discoveryFsPath.getAbsolutePath()));
        cfg.setDiscoverySpi(dspi);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024).setPersistenceEnabled(true))
                .setWalMode(WALMode.LOG_ONLY)
                .setWalCompactionEnabled(false)
                .setWalSegmentSize(WAL_SEGMENT_SIZE)
                .setCheckpointFrequency(240 * 60 * 1000)
                .setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4);
        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration cacheCfg = new CacheConfiguration(CACHE_NAME)
                .setRebalanceMode(CacheRebalanceMode.SYNC)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(1)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IGNITE_WAL_MMAP);
        super.afterTest();
    }

    public IgniteEx startClient(int idx) throws Exception {
        try {
            clientMode = true;
            return startGrid(idx);
        } finally {
            clientMode = false;
        }
    }

    public void testContinuousQueryNodeRestart() throws Exception {

        int batch = 100;
        int threads = 2;
        int restartDelay = 100;
        boolean cancel = true;

        startGrids(GRID_CNT);

        final IgniteEx load = startClient(GRID_CNT);

        load.cluster().active(true);

        try (IgniteDataStreamer<Object, Object> s = load.dataStreamer(CACHE_NAME)) {
            s.allowOverwrite(true);

            for (int i = 0; i < ENTRIES_COUNT; i++)
                s.addData(i, i);
        }

        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicInteger loadCnt = new AtomicInteger();

        IgniteInternalFuture<?> busyFut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override
            public Object call() throws Exception {
                IgniteCache<Object, Object> cache = load.cache(CACHE_NAME);
                Random rnd = ThreadLocalRandom.current();

                while (!done.get()) {
                    Map<Integer, Person> map = new TreeMap<>();

                    for (int i = 0; i < batch; i++) {
                        int key = rnd.nextInt(ENTRIES_COUNT);

                        map.put(key, new Person("fn" + key, "ln" + key));
                    }

                    cache.putAll(map);
                    loadCnt.addAndGet(map.size());
                }

                return null;
            }
        }, threads, "updater");

        ContinuousQuery<Integer, Person> qry = new ContinuousQuery<>();
        PersonListener lsnr = new PersonListener();
        qry.setLocalListener(lsnr);
//        qry.setRemoteFilter(new SerializableFilter());
        qry.setRemoteFilterFactory(factoryOf(new SerializableFilter()));

        IgniteCache<Object, Object> cache = load.cache(CACHE_NAME);

        try (QueryCursor<Cache.Entry<Integer, Person>> res = cache.query(qry)) {
            boolean keepGoing = true;
            int initCount = lsnr.count.get();
            System.out.println("+++ start with: " + initCount);
            while (keepGoing) {
                keepGoing = lsnr.count.get() < initCount + 1000;
            }
            System.out.println("+++ continue after: " + lsnr.count);
            stopGrid(2);

            System.out.println("+++ stopped.  latest count: " + lsnr.count);
            U.sleep(200L);
            System.out.println("+++ after sleep.  latest count: " + lsnr.count);

            startGrid(2);
            U.sleep(200L);

            System.out.println("+++ restarted. " +
                    " latest count: " + lsnr.count +
                    " loaded count: " + loadCnt.get());

            done.set(true);

            busyFut.get();

            System.out.println("+++ stopped publishing.  latest count: " + lsnr.count);

            U.sleep(200L);
            System.out.println("+++ final numbers... " +
                    " latest count: " + lsnr.count +
                    " loaded count: " + loadCnt.get());

        } catch (Exception e) {
            log.error("++++ Failed query ", e);
            throw e;
        }
    }

    static class Person implements Serializable {
        /**
         *
         */
        @GridToStringInclude
        @QuerySqlField(index = true, groups = "full_name")
        private String fName;

        /**
         *
         */
        @GridToStringInclude
        @QuerySqlField(index = true, groups = "full_name")
        private String lName;

        /**
         * @param fName First name.
         * @param lName Last name.
         */
        public Person(String fName, String lName) {
            this.fName = fName;
            this.lName = lName;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return S.toString(Person.class, this);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person) o;

            return Objects.equals(fName, person.fName) && Objects.equals(lName, person.lName);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            return Objects.hash(fName, lName);
        }
    }

    public static class SerializableFilter implements CacheEntryEventSerializableFilter<Integer, Person> {
        @Override
        public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends Person> cacheEntryEvent)
                throws CacheEntryListenerException {
            return true;
        }
    }

    public static class PersonListener implements CacheEntryUpdatedListener<Integer, Person> {

        private Set<Integer> entries = synchronizedSet(new HashSet<>());
        private final AtomicInteger count = new AtomicInteger();

        @Override
        public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Person>> iterable) throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends Integer, ? extends Person> evt: iterable) {
                entries.add(evt.getKey());
                count.incrementAndGet();
            }
        }
    }

}
