// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.loaddata.realtime;

import org.gridgain.examples.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.dataload.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.cache.query.GridCacheQueryType.*;
import static org.gridgain.grid.product.GridProductEdition.*;

/**
 * Real time popular words counter.
 * <p>
 * Remote nodes should always be started with configuration which includes cache
 * using following command: {@code 'ggstart.sh examples/config/example-cache-popularcounts.xml'}.
 * <p>
 * The counts are kept in cache on all remote nodes. Top {@code 10} counts from each node are
 * then grabbed to produce an overall top {@code 10} list within the grid.
 * <p>
 * NOTE: to start the example, {@code GRIDGAIN_HOME} system property or environment variable
 * must be set.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(DATA_GRID)
public class GridPopularWordsRealTimeExample {
    /** Number of most popular words to retrieve from grid. */
    private static final int POPULAR_WORDS_CNT = 10;

    /** Path to books. */
    private static final String BOOKS_PATH = "examples/java/org/gridgain/examples/datagrid/loaddata/realtime/books";

    /**
     * Starts counting words.
     *
     * @param args Command line arguments. None required.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        String ggHome = GridExamplesUtils.resolveGridGainHome();

        if (ggHome == null)
            throw new RuntimeException("GRIDGAIN_HOME must be set to GridGain installation root.");

        final File inputDir = U.resolveGridGainPath(BOOKS_PATH);

        if (inputDir == null) {
            System.err.println("Input directory does not exist: " + BOOKS_PATH);

            return;
        }

        Timer popularWordsQryTimer = new Timer("words-query-worker");

        // Start grid.
        final Grid g = GridGain.start("examples/config/example-cache-popularcounts.xml");

        try {
            TimerTask task = scheduleQuery(g, popularWordsQryTimer, POPULAR_WORDS_CNT);

            streamData(g, inputDir);

            // Force one more run to get final counts.
            task.run();

            popularWordsQryTimer.cancel();

            // Clean up caches on all nodes after run.
            g.compute().run(new Runnable() {
                @Override public void run() {
                    if (g.cache(null) == null)
                        System.err.println("Default cache not found (is example-cache-popularcounts.xml " +
                            "configuration used on all nodes?)");
                    else {
                        System.out.println("Clearing keys from cache: " + g.cache(null).size());

                        g.cache(null).clearAll();
                    }
                }
            }).get();
        }
        finally {
            GridGain.stop(true);
        }
    }

    /**
     * Populates cache in real time with words and keeps count for every word.
     *
     * @param g Grid.
     * @param inputDir Input folder.
     * @throws Exception If failed.
     */
    private static void streamData(final Grid g, final File inputDir) throws Exception {
        String[] books = inputDir.list();

        try (GridDataLoader<String, Integer> ldr = g.dataLoader(null)) {
            // Set larger per-node buffer size since our state is relatively small.
            ldr.perNodeBufferSize(2048);

            // Set custom updater which increments value for key.
            ldr.updater(new IncrementingUpdater());

            for (String name : books) {
                System.out.println(">>> Storing all words from book in in-memory data grid: " + name);

                try (BufferedReader in = new BufferedReader(new FileReader(new File(inputDir, name)))) {
                    for (String line = in.readLine(); line != null; line = in.readLine())
                        for (final String w : line.split("[^a-zA-Z0-9]"))
                            if (!w.isEmpty())
                                // Note that we are loading our closure which
                                // will then calculate proper value on remote node.
                                ldr.addData(w, 1);
                }

                System.out.println(">>> Finished storing all words from book in in-memory data grid: " + name);
            }
        }
    }

    /**
     * Schedules our popular words query to run every 3 seconds.
     *
     * @param g Grid.
     * @param timer Timer.
     * @param cnt Number of popular words to return.
     * @return Scheduled task.
     */
    private static TimerTask scheduleQuery(final Grid g, Timer timer, final int cnt) {
        TimerTask task = new TimerTask() {
            private GridCacheQuery<String, Integer> qry;

            @Override public void run() {
                try {
                    // Get reference to cache.
                    GridCache<String, Integer> cache = g.cache(null);

                    if (qry == null)
                        // Don't select words shorter than 3 letters.
                        qry = cache.queries().
                            createQuery(SQL, Integer.class, "length(_key) > 3 order by _val desc limit " + cnt);

                    List<Map.Entry<String, Integer>> results =
                        new ArrayList<>(qry.execute().get());

                    Collections.sort(results, new Comparator<Map.Entry<String, Integer>>() {
                        @Override public int compare(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
                            return e1.getValue() < e2.getValue() ? 1 : e1.getValue() > e2.getValue() ? -1 : 0;
                        }
                    });

                    for (int i = 0; i < cnt; i++) {
                        Map.Entry<String, Integer> e = results.get(i);

                        System.out.println(">>> " + e.getKey() + '=' + e.getValue());
                    }

                    System.out.println("------------");
                }
                catch (GridException e) {
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
    private static class IncrementingUpdater implements GridDataLoadCacheUpdater<String, Integer> {
        /** */
        private static final GridClosure<Integer, Integer> INC = new GridClosure<Integer, Integer>() {
            @Override public Integer apply(Integer e) {
                return e == null ? 1 : e + 1;
            }
        };

        /** {@inheritDoc} */
        @Override public void update(GridCache<String, Integer> cache,
            Collection<Map.Entry<String, Integer>> entries) throws GridException {
            for (Map.Entry<String, Integer> entry : entries)
                cache.transform(entry.getKey(), INC);
        }
    }
}
