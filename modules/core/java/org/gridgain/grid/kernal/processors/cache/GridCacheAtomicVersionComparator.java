// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import java.io.*;
import java.util.*;

/**
 * Atomic cache version comparator.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridCacheAtomicVersionComparator implements Comparator<GridCacheVersion>, Serializable {
    /** {@inheritDoc} */
    @Override public int compare(GridCacheVersion one, GridCacheVersion other) {
        int topVer = one.topologyVersion();
        int otherTopVer = other.topologyVersion();

        if (topVer == otherTopVer) {
            long globalTime = one.globalTime();
            long otherGlobalTime = other.globalTime();

            if (globalTime == otherGlobalTime) {
                long locOrder = one.order();
                long otherLocOrder = other.order();

                if (locOrder == otherLocOrder) {
                    int nodeOrder = one.nodeOrder();
                    int otherNodeOrder = other.nodeOrder();

                    return nodeOrder == otherNodeOrder ? 0 : nodeOrder < otherNodeOrder ? -1 : 1;
                }
                else
                    return locOrder > otherLocOrder ? 1 : -1;
            }
            else
                return globalTime > otherGlobalTime ? 1 : -1;
        }
        else
            return topVer > otherTopVer ? 1 : -1;
    }
}
