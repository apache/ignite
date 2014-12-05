/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.h2indexing;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.marshaller.optimized.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.spi.indexing.h2.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.IgniteEventType.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Indexing performance test.
 */
public class GridH2IndexingSpiLoadTest {
    /**
     * Gets property as {@code int}.
     *
     * @param p Properties.
     * @param key Key.
     * @param dflt Default value.
     * @return Value for key.
     */
    private static int getInt(Properties p, String key, int dflt) {
        String val = p.getProperty(key);

        return val == null ? dflt : Integer.parseInt(val);
    }

    /**
     * Gets property as long.
     *
     * @param p Properties.
     * @param key Key.
     * @param dflt Default value.
     * @return Value for key.
     */
    private static long getLong(Properties p, String key, long dflt) {
        String val = p.getProperty(key);

        return val == null ? dflt : Long.parseLong(val);
    }

    /**
     * Loads configuration for test.
     *
     * @return Properties.
     * @throws IOException if failed.
     */
    private static Properties loadProperties() throws IOException {
        Properties p = new Properties();

        File propsFile = new File("idx-test.properties");

        if (propsFile.exists() && propsFile.isFile())
            p.load(new FileReader(propsFile));
        else
            System.out.println("Properties file not found.");

        return p;
    }

    /**
     * Main method.
     *
     * @param args Arguments.
     * @throws GridException If failed.
     * @throws IOException If failed.
     */
    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String...args) throws GridException, IOException {
        final Properties p = loadProperties();

        final int MAX_SIZE = getInt(p, "max-entries-count", 500000);
        final int MAX_NAMES = getInt(p, "max-names-count", 100);

        IgniteConfiguration c = new IgniteConfiguration();

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(discoSpi);

        IgniteOptimizedMarshaller m = new IgniteOptimizedMarshaller();

        m.setClassNames(F.asList(GridTestEntity.class.getName()));

        c.setMarshaller(m);

        GridH2IndexingSpi indexing = new GridH2IndexingSpi();

        long offheap = getLong(p, "offheap-size", 3000000000L);

        if (1 == getInt(p, "offheap-enabled", 1))
            indexing.setMaxOffHeapMemory(offheap);

        boolean enableIndexing = 1 == getInt(p, "indexing-enabled", 1);

        if (enableIndexing)
            c.setIndexingSpi(indexing);

        GridCacheConfiguration cc = new GridCacheConfiguration();

        cc.setName("local");
        cc.setCacheMode(GridCacheMode.LOCAL);
        cc.setStartSize(MAX_SIZE);
        cc.setWriteSynchronizationMode(FULL_SYNC);
       // TODO enable evictions
//        cc.setEvictionPolicy(new GridCacheFifoEvictionPolicy(getInt(p, "not-evict-count", 100000)));
        cc.setQueryIndexEnabled(enableIndexing);
        cc.setDistributionMode(PARTITIONED_ONLY);
        cc.setStoreValueBytes(false);
        cc.setSwapEnabled(false);
        cc.setOffHeapMaxMemory(0);

        c.setCacheConfiguration(cc);
        c.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        final Ignite g = G.start(c);

        final GridCache<Long, GridTestEntity> cache = g.cache("local");

        Random rnd = new Random();

        for (int i = 0; i < MAX_SIZE; i++) {
            if (rnd.nextBoolean())
                cache.put((long)i, new GridTestEntity(Integer.toHexString(rnd.nextInt(MAX_NAMES)), new Date()));

            if (i % 10000 == 0)
                System.out.println(i);
        }

        List<Runner> rs = F.asList(
            new Runner("put", 1000, getInt(p, "put-threads", 8)) {
                @Override
                protected void doRun(Random rnd) throws Exception {
                    cache.put((long)rnd.nextInt(MAX_SIZE), new GridTestEntity(Integer.toHexString(rnd.nextInt(MAX_NAMES)),
                        null));
                }
            }.start(),

            new Runner("remove", 1000, getInt(p, "remove-threads", 2)) {
                @Override
                protected void doRun(Random rnd) throws Exception {
                    cache.remove((long)rnd.nextInt(MAX_SIZE));
                }
            }.start(),

            new Runner("query", 10, getInt(p, "query-threads", 8)) {
                @Override
                protected void doRun(Random rnd) throws Exception {
                    GridCacheQuery<Map.Entry<Long, GridTestEntity>> qry = cache.queries().createSqlQuery(
                        GridTestEntity.class, "name = ?");

                    qry.execute(Integer.toHexString(rnd.nextInt(MAX_NAMES))).get();

//                    U.sleep(getInt(p, "query-sleep-time", 25));
                }
            }.start());

        for(;;) {
            U.sleep(getInt(p, "print-period", 3000));

            for (Runner r : rs)
                r.print();

            long bytes = indexing.getAllocatedOffHeapMemory();

            System.out.println("offheap bytes: " + bytes);

            System.out.println();
        }
    }

    /**
     * Runs job in loop.
     */
    private abstract static class Runner implements Runnable {
        /** */
        private final String name;

        /** */
        private final int iters;

        /** */
        private final int threads;

        /** */
        private AtomicLong cntr = new AtomicLong();

        /** */
        private long lastPrintTime;
        /**
         * Constructor.
         *
         * @param name Name.
         * @param iters Iterations count to measure.
         * @param threads Threads.
         */
        protected Runner(String name, int iters, int threads) {
            this.name = name;
            this.iters = iters;
            this.threads = threads;
        }

        /**
         * Start runner.
         *
         * @return Self.
         */
        Runner start() {
            Thread[] ths = new Thread[threads];

            for (int i = 0; i < threads; i++) {
                ths[i] = new Thread(this, name + "-" + i);

                ths[i].setDaemon(true);

                ths[i].start();
            }

            lastPrintTime = System.currentTimeMillis();

            return this;
        }

        /** */
        @SuppressWarnings("InfiniteLoopStatement")
        @Override public void run() {
            Random rnd = new Random();

            try {
                for (;;) {
                    for (int i = 0; i < iters; i++)
                        doRun(rnd);

                    cntr.addAndGet(iters);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * Print stats.
         */
        void print() {
            long cnt = cntr.get();
            long time = System.currentTimeMillis();

            X.println("-- " + name + ": " + (cnt * 1000 / (time - lastPrintTime)) + " op/sec");

            lastPrintTime = time;

            cntr.addAndGet(-cnt);
        }

        /**
         * Do the job.
         *
         * @param rnd Random for current thread.
         * @throws Exception If failed.
         */
        protected abstract void doRun(Random rnd) throws Exception;
    }
}
