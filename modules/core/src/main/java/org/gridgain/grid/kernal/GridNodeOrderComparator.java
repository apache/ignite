/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;

import java.io.*;
import java.util.*;

/**
 * Node order comparator.
 */
public class GridNodeOrderComparator implements Comparator<ClusterNode>, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public int compare(ClusterNode n1, ClusterNode n2) {
        return n1.order() < n2.order() ? -1 : n1.order() > n2.order() ? 1 : n1.id().compareTo(n2.id());
    }
}
