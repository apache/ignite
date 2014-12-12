/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.processors.cache.*;

/**
 *
 */
public class GridCacheProjectionRemoveTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testRemove() throws IgniteCheckedException {
        cache().put("key", 1);

        assert cache().remove("key", 1);
        assert !cache().remove("key", 1);
    }
}
