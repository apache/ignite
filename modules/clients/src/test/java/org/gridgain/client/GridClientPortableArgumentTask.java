/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.gridgain.grid.*;

/**
 * Task where argument and result are {@link org.gridgain.client.GridClientTestPortable}.
 */
public class GridClientPortableArgumentTask extends GridTaskSingleJobSplitAdapter {
    /** {@inheritDoc} */
    @Override protected Object executeJob(int gridSize, Object arg) throws GridException {
        GridClientTestPortable p = (GridClientTestPortable)arg;

        return new GridClientTestPortable(p.i + 1, true);
    }
}
