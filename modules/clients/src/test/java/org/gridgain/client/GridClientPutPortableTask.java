/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.apache.ignite.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;

/**
 * Task creates portable object and puts it in cache.
 */
public class GridClientPutPortableTask extends GridTaskSingleJobSplitAdapter {
    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected Object executeJob(int gridSize, Object arg) throws IgniteCheckedException {
        String cacheName = (String)arg;

        GridCache<Object, Object> cache = ignite.cache(cacheName);

        GridClientTestPortable p = new GridClientTestPortable(100, true);

        cache.put(1, p);

        return true;
    }
}
