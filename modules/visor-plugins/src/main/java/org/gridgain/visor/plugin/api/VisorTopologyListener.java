/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.visor.plugin.api;

import java.util.*;

/**
 * Listener for grid node topology changes.
 */
public interface VisorTopologyListener {
    /**
     * Action that should be done on node join.
     *
     * @param nid ID of node that join topology.
     */
    public void onNodeJoin(UUID nid);

    /**
     * Action that should be done on node left.
     *
     * @param nid ID of node that left topology.
     */
    public void onNodeLeft(UUID nid);

    /**
     * Action that should be done on node failed.
     *
     * @param nid ID of failed node.
     */
    public void onNodeFailed(UUID nid);

    /**
     * Action that should be done on node segmented.
     *
     * @param nid ID of segmented node.
     */
    public void onNodeSegmented(UUID nid);
}
