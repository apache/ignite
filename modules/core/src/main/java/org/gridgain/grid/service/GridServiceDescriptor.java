// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.service;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridServiceDescriptor extends Serializable {
    public String name();

    public Class<? extends GridService> serviceClass();

    public int totalCount();

    public int maxPerNodeCount();

    @Nullable public String cacheName();

    @Nullable public <K> K affinityKey();

    /**
     *
     * @return Map of number of service instances per node.
     */
    public Map<GridNode, Integer> topologySnapshot();
}
