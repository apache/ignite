/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Tests for fields queries.
 */
public class GridCacheAtomicFieldsQuerySelfTest extends GridCachePartitionedFieldsQuerySelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    /** {@inheritDoc} */
    @Override public void testCacheMetaData() throws Exception {
        // No-op.
    }

    /**
     *
     */
    public void testUnsupportedOperations() {
        GridCacheQuery<List<?>> qry = grid(0).cache(null).queries().createSqlFieldsQuery(
            "update Person set name = ?");

        try {
            qry.execute("Mary Poppins").get();

            fail("We don't support updates.");
        }
        catch (IgniteCheckedException e) {
            X.println("___ " + e.getMessage());
        }
    }
}
