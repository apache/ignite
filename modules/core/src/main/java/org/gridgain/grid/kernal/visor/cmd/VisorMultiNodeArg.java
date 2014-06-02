/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd;

import java.io.*;
import java.util.*;

/**
 * Base class for Visor task arguments intended to query data from multiple nodes.
 */
public class VisorMultiNodeArg implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final Set<UUID> nodeIds;

    /**
     * Create task argument with specified nodes Ids.
     *
     * @param nids Nodes Ids.
     */
    public VisorMultiNodeArg(Set<UUID> nids) {
        nodeIds = nids;
    }

    /**
     * @return Node ids.
     */
    public Set<UUID> nodeIds() {
        return nodeIds;
    }
}

