// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.affinity;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Hash ID resolver which uses generated node ID as alternate node ID. As new node ID generated on each node start
 * this resolver do not provide ability to put restarted node into the same place on the hash ring.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheAffinityNodeIdHashResolver implements GridCacheAffinityNodeHashResolver {
    /** {@inheritDoc} */
    @Override public Object resolve(GridNode node) {
        return node.id();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheAffinityNodeIdHashResolver.class, this);
    }
}
