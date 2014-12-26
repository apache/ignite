/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.dataload.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;

import javax.cache.processor.*;
import java.util.*;

/**
 * Real time popular numbers counter.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ggstart.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start GridGain node with {@code examples/config/example-cache.xml} configuration.
 */
public class CachePopularNumbersExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /** Count of most popular numbers to retrieve from grid. */
    private static final int POPULAR_NUMBERS_CNT = 10;

    /** Random number generator. */
    private static final Random RAND = new Random();

    /** Range within which to generate numbers. */
    private static final int RANGE = 1000;

    /** Count of total numbers to generate. */
    private static final int CNT = 1000000;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        Timer popularNumbersQryTimer = new Timer("numbers-query-worker");

        try (Ignite g = Ignition.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache popular numbers example started.");

            // Clean up caches on all nodes before run.
            g.cache(CACHE_NAME).globalClearAll(0);

            ClusterGroup prj = g.cluster().forCache(CACHE_NAME);

            if (prj.nodes().isEmpty()) {
                System.out.println("Grid does not have cache configured: " + CACHE_NAME);

                return;
            }

            TimerTask task = scheduleQuery(g, popularNumbersQryTimer, POPULAR_NUMBERS_CNT);

            streamData(g);

            // Force one more run to get final counts.
            task.run();

            popularNumbersQryTimer.cancel();
        }
    }

    /**
     * Populates cache in real time with numbers and keeps count for every number.
     *
     * @param g Grid.
     * @throws IgniteCheckedException If failed.
     */
    private static void streamData(final Ignite g) throws IgniteCheckedException {
        try (IgniteDataLoader<Integer, Long> ldr = g.dataLoader(CACHE_NAME)) {
            // Set larger per-node buffer size since our state is relatively small.
            ldr.perNodeBufferSize(2048);

            ldr.updater(new IncrementingUpdater());

            for (int i = 0; i < CNT; i++)
                ldr.addData(RAND.nextInt(RANGE), 1L);
        }
    }

    /**
     * Schedules our popular numbers query to run every 3 seconds.
     *
     * @param g Grid.
     * @param timer Timer.
     * @param cnt Number of popular numbers to return.
     * @return Scheduled task.
     */
    private static TimerTask scheduleQuery(final Ignite g, Timer timer, final int cnt) {
        TimerTask task = new TimerTask() {
            private GridCacheQuery<List<?>> qry;

            @Override public void run() {
                // Get reference to cache.
                GridCache<Integer, Long> cache = g.cache(CACHE_NAME);

                if (qry == null)
                    qry = cache.queries().
                        createSqlFieldsQuery("select _key, _val from Long order by _val desc limit " + cnt);

                try {
                    List<List<?>> results = new ArrayList<>(qry.execute().get());

                    Collections.sort(results, new Comparator<List<?>>() {
                        @Override public int compare(List<?> r1, List<?> r2) {
                            long cnt1 = (Long)r1.get(1);
                            long cnt2 = (Long)r2.get(1);

                            return cnt1 < cnt2 ? 1 : cnt1 > cnt2 ? -1 : 0;
                        }
                    });

                    for (int i = 0; i < cnt && i < results.size(); i++) {
                        List<?> res = results.get(i);

                        System.out.println(res.get(0) + "=" + res.get(1));
                    }

                    System.out.println("----------------");
                }
                catch (IgniteCheckedException e) {
                    e.printStackTrace();
                }
            }
        };

        timer.schedule(task, 3000, 3000);

        return task;
    }

    /**
     * Increments value for key.
     */
    private static class IncrementingUpdater implements IgniteDataLoadCacheUpdater<Integer, Long> {
        /** */
        private static final EntryProcessor<Integer, Long, Void> INC = new EntryProcessor<Integer, Long, Void>() {
            @Override public Void process(MutableEntry<Integer, Long> e, Object... args) {
                Long val = e.getValue();

                e.setValue(val == null ? 1 : val + 1);

                return null;
            }
        };

        /** {@inheritDoc} */
        @Override public void update(IgniteCache<Integer, Long> cache, Collection<Map.Entry<Integer, Long>> entries) {
            for (Map.Entry<Integer, Long> entry : entries)
                cache.invoke(entry.getKey(), INC);
        }
    }
}
