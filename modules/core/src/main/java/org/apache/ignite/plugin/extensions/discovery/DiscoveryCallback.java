/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.plugin.extensions.discovery;

import org.apache.ignite.cluster.*;
import org.apache.ignite.plugin.*;

import java.util.*;

/**
 * Plugin extension that allows to listen messages from discovery.
 *
 * TODO 9447: redesign.
 */
public interface DiscoveryCallback extends IgniteExtension {
    /**
     * Handles node start.
     *
     * @param remoteNodes Remote grid nodes.
     */
    public void onStart(Collection<ClusterNode> remoteNodes);

    /**
     * Handles node joined event.
     *
     * @param node Joined node.
     */
    public void beforeNodeJoined(ClusterNode node);

    /**
     * Handles node left event.
     *
     * @param node Left node.
     */
    public void onNodeLeft(ClusterNode node);
}
