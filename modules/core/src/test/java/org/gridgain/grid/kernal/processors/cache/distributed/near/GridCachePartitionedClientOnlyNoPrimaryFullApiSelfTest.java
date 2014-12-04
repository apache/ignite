/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

/**
 * Tests for local cache.
 */
public class GridCachePartitionedClientOnlyNoPrimaryFullApiSelfTest extends GridCachePartitionedFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return GridCacheDistributionMode.CLIENT_ONLY;
    }

    /**
     *
     */
    public void testMapKeysToNodes() {
        cache().affinity().mapKeysToNodes(Arrays.asList("1", "2"));
    }

    /**
     *
     */
    public void testMapKeyToNode() {
        assert cache().affinity().mapKeyToNode("1") == null;
    }

    /**
     * @return Handler that discards grid exceptions.
     */
    @Override protected IgniteClosure<Throwable, Throwable> errorHandler() {
        return new IgniteClosure<Throwable, Throwable>() {
            @Override public Throwable apply(Throwable e) {
                if (e instanceof GridException || X.hasCause(e, GridTopologyException.class)) {
                    info("Discarding exception: " + e);

                    return null;
                }
                else
                    return e;
            }
        };
    }
}
