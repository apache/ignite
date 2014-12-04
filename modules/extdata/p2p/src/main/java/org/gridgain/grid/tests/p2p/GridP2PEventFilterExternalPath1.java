/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.tests.p2p;

import org.apache.ignite.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;

/**
 * Simple event filter
 */
@SuppressWarnings({"ProhibitedExceptionThrown"})
public class GridP2PEventFilterExternalPath1 implements IgnitePredicate<IgniteEvent> {
    /** */
    @GridUserResource
    private transient GridTestUserResource rsrc;

    /** Instance of grid. Used for save class loader and injected resource. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public boolean apply(IgniteEvent evt) {
        try {
            int[] res = new int[] {
                System.identityHashCode(rsrc),
                System.identityHashCode(getClass().getClassLoader())
            };

            ignite.message(ignite.cluster().forRemotes()).send(null, res);
        }
        catch (GridException e) {
            throw new RuntimeException(e);
        }

        return true;
    }
}
