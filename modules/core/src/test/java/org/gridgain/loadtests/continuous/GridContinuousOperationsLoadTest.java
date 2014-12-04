/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.continuous;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.query.continuous.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.loadtests.util.GridLoadTestArgs.*;
import static org.gridgain.testframework.GridLoadTestUtils.*;
import static org.gridgain.testframework.GridTestUtils.*;

/**
 * Load test for {@link GridCacheContinuousQuery}.
 */
public class GridContinuousOperationsLoadTest {
    /**
     * Main method.
     *
     * @param args Command line arguments.
     * @throws Exception If error occurs.
     */
    public static void main(String[] args) throws Exception {
        final String cfgPath = args.length > 0 ? args[0] : "examples/config/example-cache.xml";
        final String cacheName = getStringProperty(CACHE_NAME, "partitioned");
        final Integer valSize = getIntProperty(VALUE_SIZE, 1024);
        final Integer threadsCnt = getIntProperty(THREADS_CNT, 8);
        final Integer testDurSec = getIntProperty(TEST_DUR_SEC, 180);

        final Integer filterSkipProb = getIntProperty("GG_FILTER_SKIP_PROBABILITY", 10, new C1<Integer, String>() {
            @Nullable @Override public String apply(Integer val) {
                if (val < 0 || val > 100)
                    return "The value should be between 1 and 100.";

                return null;
            }
        });

        final boolean useQry = getBooleanProperty("GG_USE_QUERIES", true);
        final int bufSize = getIntProperty("GG_BUFFER_SIZE", 1);
        final long timeInterval = getLongProperty("GG_TIME_INTERVAL", 0);
        final int parallelCnt = getIntProperty("GG_PARALLEL_COUNT", 8);
        final int keyRange = getIntProperty("GG_KEY_RANGE", 100000);
        final long updSleepMs = getLongProperty("GG_UPDATE_SLEEP_MS", 0);
        final long filterSleepMs = getLongProperty("GG_FILTER_SLEEP_MS", 0);
        final long cbSleepMs = getLongProperty("GG_CALLBACK_SLEEP_MS", 0);

        X.println("The test will start with the following parameters:");

        dumpProperties(System.out);

        try (Ignite ignite = GridGain.start(cfgPath)) {
            final GridCache<Object, Object> cache = ignite.cache(cacheName);

            if (cache == null)
                throw new GridException("Cache is not configured: " + cacheName);

            // Continuous query manager, used to monitor queue size.
            final GridCacheContinuousQueryManager contQryMgr =
                ((GridCacheAdapter)((GridCacheProxyImpl)cache).cache()).context().continuousQueries();

            if (contQryMgr == null)
                throw new GridException("Could not access GridCacheContinuousQueryManager");

            final AtomicBoolean stop = new AtomicBoolean(); // Stop flag.
            final AtomicLong cbCntr = new AtomicLong();     // Callback counter.
            final AtomicLong updCntr = new AtomicLong();    // Update counter.

            for (int i = 0; i < parallelCnt; i++) {
                if (useQry) {
                    GridCacheContinuousQuery<Object, Object> qry = cache.queries().createContinuousQuery();

                    qry.callback(new PX2<UUID, Collection<Map.Entry<Object, Object>>>() {
                        @Override public boolean applyx(UUID uuid, Collection<Map.Entry<Object, Object>> entries)
                            throws GridInterruptedException {
                            if (cbSleepMs > 0)
                                U.sleep(cbSleepMs);

                            cbCntr.addAndGet(entries.size());

                            return true; // Continue listening.
                        }
                    });

                    qry.filter(new PX2<Object, Object>() {
                        @Override public boolean applyx(Object key, Object val) throws GridInterruptedException {
                            if (filterSleepMs > 0)
                                U.sleep(filterSleepMs);

                            return Math.random() * 100 >= filterSkipProb;
                        }
                    });

                    qry.bufferSize(bufSize);
                    qry.timeInterval(timeInterval);

                    qry.execute();
                }
                else {
                    ignite.events().remoteListen(
                        bufSize,
                        timeInterval,
                        true,
                        new PX2<UUID, GridEvent>() {
                            @Override
                            public boolean applyx(UUID uuid, GridEvent evt)
                                throws GridInterruptedException {
                                if (cbSleepMs > 0)
                                    U.sleep(cbSleepMs);

                                cbCntr.incrementAndGet();

                                return true; // Continue listening.
                            }
                        },
                        new PX1<GridEvent>() {
                            @Override
                            public boolean applyx(GridEvent evt) throws GridInterruptedException {
                                if (filterSleepMs > 0)
                                    U.sleep(filterSleepMs);

                                return Math.random() * 100 >= filterSkipProb;
                            }
                        },
                        EVT_CACHE_OBJECT_PUT
                    );
                }
            }

            // Start collector thread.
            startDaemon(new Runnable() {
                @Override public void run() {
                    try {
                        while (!stop.get() && !Thread.currentThread().isInterrupted()) {
                            long cbCntr0 = cbCntr.get();
                            long updCntr0 = updCntr.get();

                            U.sleep(1000);

                            long cbDelta = cbCntr.get() - cbCntr0;
                            long updDelta = updCntr.get() - updCntr0;

                            X.println("Stats [entriesPerSec=" + cbDelta +
                                ", updatesPerSec=" + updDelta + ']');
                        }
                    }
                    catch (GridInterruptedException ignored) {
                        // No-op.
                    }
                }
            });

            X.println("Starting " + threadsCnt + " generator thread(s).");

            // Start generator threads.
            IgniteFuture<Long> genFut = runMultiThreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    byte[] val = new byte[valSize];
                    ThreadLocalRandom8 rnd = ThreadLocalRandom8.current();

                    while (!stop.get() && !Thread.currentThread().isInterrupted()) {
                        Integer key = rnd.nextInt(keyRange);

                        cache.putx(key, val);

                        updCntr.incrementAndGet();

                        if (updSleepMs > 0)
                            U.sleep(updSleepMs);
                    }

                    return true;
                }
            }, threadsCnt, "load-test-generator");

            U.sleep(testDurSec * 1000);

            stop.set(true);

            genFut.get();
        }
    }
}
