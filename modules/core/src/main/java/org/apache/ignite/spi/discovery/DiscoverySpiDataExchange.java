/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.discovery;

import java.util.*;

/**
 * Handler for initial data exchange between GridGain nodes. Data exchange
 * is initiated by a new node when it tries to join topology and finishes
 * before it actually joins.
 */
public interface DiscoverySpiDataExchange {
    /**
     * Collects data from all components. This method is called both
     * on new node that joins topology to transfer its data to existing
     * nodes and on all existing nodes to transfer their data to new node.
     *
     * @param nodeId ID of new node that joins topology.
     * @return Collection of discovery data objects from different components.
     */
    public List<Object> collect(UUID nodeId);

    /**
     * Notifies discovery manager about data received from remote node.
     *
     * @param data Collection of discovery data objects from different components.
     */
    public void onExchange(List<Object> data);
}
