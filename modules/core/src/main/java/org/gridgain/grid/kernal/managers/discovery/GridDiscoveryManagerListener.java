/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.discovery;

import org.gridgain.grid.*;

import java.util.*;

/**
 * Plugin extension that allow to listen messages from discovery manager.
 */
public interface GridDiscoveryManagerListener {
    /**
     * Handles node start.
     *
     * @param remoteNodes Remote grid nodes.
     */
    public void onStart(Collection<GridNode> remoteNodes);

    /**
     * Handles node joined event.
     *
     * @param node Joined node.
     */
    public void beforeNodeJoined(GridNode node);

    /**
     * Handles node left event.
     *
     * @param node Left node.
     */
    public void onNodeLeft(GridNode node);
}
