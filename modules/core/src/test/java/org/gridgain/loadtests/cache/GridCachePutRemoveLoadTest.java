/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.cache;

import com.beust.jcommander.*;
import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.lru.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMemoryMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * The benchmark that performs put and remove operations on the cache to identify memory leaks.
 * <P>
 * Run this class with needed parameters. Type '-help' to get the list of the available parameters.
 */
public class GridCachePutRemoveLoadTest {
    /** */
    private final Arguments args;

    /** */
    private GridCache<Object, Object> cache;

    /**
     * @param args Arguments.
     */
    public GridCachePutRemoveLoadTest(Arguments args) {
        this.args = args;
    }

    /**
     * @param a Arguments.
     */
    public static void main(String[] a) {
        Arguments args = new Arguments();

        JCommander jCommander = new JCommander();

        jCommander.setAcceptUnknownOptions(true);
        jCommander.addObject(args);

        jCommander.parse(a);

        if (args.help()) {
            jCommander.usage();

            return;
        }

        X.println(args.toString());

        GridCachePutRemoveLoadTest test = new GridCachePutRemoveLoadTest(args);

        try {
            test.startNodes();

            test.runTest();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            G.stopAll(true);
        }
    }

    /**
     * @throws Exception If failed.
     */
    protected void startNodes() throws Exception {
        for (int i = 0; i < args.nodes(); i++) {
            GridConfiguration cfg =
                GridGainEx.loadConfiguration("modules/core/src/test/config/spring-cache-put-remove-load.xml").get1();

            assert cfg != null;

            cfg.setGridName("g" + i);

            GridCacheConfiguration cacheCfg = cfg.getCacheConfiguration()[0];

            GridCacheDistributionMode distro = i == 0 &&
                args.distribution() == CLIENT_ONLY ? CLIENT_ONLY : PARTITIONED_ONLY;

            cacheCfg.setCacheMode(args.cache());
            cacheCfg.setDistributionMode(distro);
            cacheCfg.setWriteSynchronizationMode(args.synchronization());
            cacheCfg.setAtomicWriteOrderMode(args.orderMode());

            if (cacheCfg.getCacheMode() == GridCacheMode.PARTITIONED)
                cacheCfg.setBackups(args.backups());

            if (args.isOffHeap()) {
                cacheCfg.setOffHeapMaxMemory(0);

                if (args.isOffheapValues())
                    cacheCfg.setMemoryMode(OFFHEAP_VALUES);
            }

            cacheCfg.setAtomicityMode(args.transactional() ? TRANSACTIONAL : ATOMIC);

            if (args.evictionEnabled())
                cacheCfg.setEvictionPolicy(new GridCacheLruEvictionPolicy(1000));

            G.start(cfg);
        }

        Ignite g = G.grid("g0");

        assert g != null;

        cache = g.cache("cache");

        assert cache != null;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("BusyWait")
    private void runTest() throws Exception {
        X.println(">>>");
        X.println(">>> Running test.");
        X.println(">>>");

        final AtomicLong putNum = new AtomicLong();

        final AtomicLong rmvNum = new AtomicLong();

        Thread timer = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        long rmv = rmvNum.get();

                        long put = putNum.get();

                        if (args.evictionEnabled())
                            X.println("Put: " + put);
                        else
                            X.println("Put: " + put + ", removed: " + rmv);

                        Thread.sleep(5000);
                    }
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        timer.setDaemon(true);
        timer.start();

        int queueSize = 100000;

        final BlockingQueue<Long> queue = new ArrayBlockingQueue<>(queueSize);

        if (!args.evictionEnabled()) {
            Thread rmvThread = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        for (long i = 0; i < Long.MAX_VALUE; i++) {
                            Long key = queue.take();

                            cache.removex(key);

                            rmvNum.set(key);
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, "rmvThread");

            rmvThread.start();
        }

        for (long i = 0; i < Long.MAX_VALUE; i++) {
            cache.putx(i, i);

            putNum.set(i);

            if (!args.evictionEnabled()) {
                // Wait for queue to be empty if remove operation is slower than put operation.
                if (!queue.offer(i)) {
                    while (!queue.isEmpty())
                        Thread.sleep(1000);

                    X.println("Waited for the remover thread to empty the queue.");

                    queue.offer(i);
                }
            }
        }
    }

    /**
     *
     */
    private static class Arguments {
        /** Main arguments (arguments without prefix '-') fall here. */
        @Parameter(description = "Main arguments")
        @GridToStringExclude
        private Iterable<String> mainArgs = new ArrayList<>();

        /** */
        @Parameter(names = "-n", description = "Nodes")
        private int nodes = 1;

        /** */
        @Parameter(names = "-cm", description = "Cache Mode")
        private GridCacheMode cacheMode = GridCacheMode.PARTITIONED;

        /** */
        @Parameter(names = "-sm", description = "Synchronization Mode")
        private GridCacheWriteSynchronizationMode syncMode = GridCacheWriteSynchronizationMode.PRIMARY_SYNC;

        /** */
        @Parameter(names = "-wo", description = "Write Ordering Mode")
        private GridCacheAtomicWriteOrderMode orderMode = GridCacheAtomicWriteOrderMode.CLOCK;

        /** */
        @Parameter(names = "-dm", description = "Distribution mode")
        private GridCacheDistributionMode distroMode = PARTITIONED_ONLY;

        /** */
        @Parameter(names = "-ot", description = "Tiered Offheap")
        private boolean offheapTiered;

        /** */
        @Parameter(names = "-ov", description = "Offheap Values Only")
        private boolean offheapVals;

        /** */
        @Parameter(names = "-b", description = "Backups")
        private int backups;

        /** */
        @Parameter(names = "-tx", description = "Whether transactional cache is used or not")
        private boolean tx;

        /** */
        @Parameter(names = "-ee", description = "Eviction Enabled")
        private boolean evictionEnabled;

        /** */
        @Parameter(names = "-help", description = "Print this help message")
        private boolean help;

        /**
         * @return If help requested.
         */
        public boolean help() {
            return help;
        }

        /**
         * @return Distribution.
         */
        public GridCacheDistributionMode distribution() {
            return distroMode;
        }

        /**
         * @return Cache Mode.
         */
        public GridCacheMode cache() {
            return cacheMode;
        }

        /**
         * @return Synchronization.
         */
        public GridCacheWriteSynchronizationMode synchronization() {
            return syncMode;
        }

        /**
         * @return Cache write ordering mode.
         */
        public GridCacheAtomicWriteOrderMode orderMode() {
            return orderMode;
        }

        /**
         * @return Backups.
         */
        public int backups() {
            return backups;
        }

        /**
         * @return Offheap tiered.
         */
        public boolean isOffheapTiered() {
            return offheapTiered;
        }

        /**
         * @return Offheap values.
         */
        public boolean isOffheapValues() {
            return offheapVals;
        }

        /**
         * @return {@code True} if any offheap is enabled.
         */
        public boolean isOffHeap() {
            return offheapTiered || offheapVals;
        }

        /**
         * @return Nodes.
         */
        public int nodes() {
            return nodes;
        }

        /**
         * @return Whether transactional cache is used or not.
         */
        public boolean transactional() {
            return tx;
        }

        /**
         * @return Eviction enabled.
         */
        public boolean evictionEnabled() {
            return evictionEnabled;
        }

        /**
         * @return Main arguments.
         */
        public Iterable<String> mainArgs() {
            return mainArgs;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Arguments.class, this);
        }
    }
}
