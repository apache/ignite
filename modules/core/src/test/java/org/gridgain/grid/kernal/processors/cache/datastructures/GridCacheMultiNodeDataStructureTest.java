/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.util.typedef.*;

/**
 * Starts up an node with cache configuration.
 * You can also must start a stand-alone GridGain instance by passing the path
 * to configuration file to {@code 'ggstart.{sh|bat}'} script, like so:
 * {@code 'ggstart.sh examples/config/example-cache.xml'}.
 */
public class GridCacheMultiNodeDataStructureTest {
    /** Ensure singleton. */
    private GridCacheMultiNodeDataStructureTest() { /* No-op. */ }

    /**
     * Put data to cache and then queries them.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        try (Grid g = G.start("examples/config/example-cache.xml")) {
            // All available nodes.
            if (g.cluster().nodes().size() <= 2)
                throw new GridException("At least 2 nodes must be started.");

            sample(g, "partitioned");
            sample(g, "replicated");
            sample(g, "local");
        }
    }

    /**
     *
     * @param g Grid.
     * @param cacheName Cache name.
     * @throws GridException If failed.
     */
    private static void sample(Grid g, String cacheName) throws GridException {
        GridCache<Long, Object> cache = g.cache(cacheName);

        GridCacheAtomicLong atomicLong = cache.dataStructures().atomicLong("keygen", 0, true);

        GridCacheAtomicSequence seq = cache.dataStructures().atomicSequence("keygen", 0, true);

        seq.incrementAndGet();
        seq.incrementAndGet();

        seq.incrementAndGet();
        seq.incrementAndGet();

        atomicLong.incrementAndGet();
        atomicLong.incrementAndGet();
        atomicLong.incrementAndGet();

        X.println(cacheName+": Seq: " + seq.get() + " atomicLong " + atomicLong.get());
    }
}
