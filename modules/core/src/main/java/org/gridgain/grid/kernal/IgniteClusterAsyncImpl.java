/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class IgniteClusterAsyncImpl extends IgniteAsyncSupportAdapter implements IgniteCluster {
    /** */
    private final GridKernal grid;

    /**
     * @param grid Grid.
     */
    public IgniteClusterAsyncImpl(GridKernal grid) {
        super(true);

        this.grid = grid;
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
        return grid.localNode();
    }

    /** {@inheritDoc} */
    @Override public GridProjection forLocal() {
        return grid.forLocal();
    }

    /** {@inheritDoc} */
    @Override public <K, V> ClusterNodeLocalMap<K, V> nodeLocalMap() {
        return grid.nodeLocalMap();
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        return grid.pingNode(nodeId);
    }

    /** {@inheritDoc} */
    @Override public long topologyVersion() {
        return grid.topologyVersion();
    }

    /** {@inheritDoc} */
    @Nullable @Override public Collection<ClusterNode> topology(long topVer) {
        return grid.topology(topVer);
    }

    /** {@inheritDoc} */
    @Override public <K> Map<ClusterNode, Collection<K>> mapKeysToNodes(@Nullable String cacheName,
        @Nullable Collection<? extends K> keys) throws GridException {
        return grid.mapKeysToNodes(cacheName, keys);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K> ClusterNode mapKeyToNode(@Nullable String cacheName, K key) throws GridException {
        return grid.mapKeyToNode(cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridTuple3<String, Boolean, String>> startNodes(File file,
        boolean restart, int timeout, int maxConn) throws GridException {
        return saveOrGet(grid.startNodesAsync(file, restart, timeout, maxConn));
    }

    /** {@inheritDoc} */
    @Override public Collection<GridTuple3<String, Boolean, String>> startNodes(
        Collection<Map<String, Object>> hosts, @Nullable Map<String, Object> dflts, boolean restart, int timeout,
        int maxConn) throws GridException {
        return saveOrGet(grid.startNodesAsync(hosts, dflts, restart, timeout, maxConn));
    }

    /** {@inheritDoc} */
    @Override public void stopNodes() throws GridException {
        grid.stopNodes();
    }

    /** {@inheritDoc} */
    @Override public void stopNodes(Collection<UUID> ids) throws GridException {
        grid.stopNodes(ids);
    }

    /** {@inheritDoc} */
    @Override public void restartNodes() throws GridException {
        grid.restartNodes();
    }

    /** {@inheritDoc} */
    @Override public void restartNodes(Collection<UUID> ids) throws GridException {
        grid.restartNodes(ids);
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        grid.resetMetrics();
    }

    /** {@inheritDoc} */
    @Override public IgniteCluster enableAsync() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public Ignite grid() {
        return grid.grid();
    }

    /** {@inheritDoc} */
    @Override public GridProjection forNodes(Collection<? extends ClusterNode> nodes) {
        return grid.forNodes(nodes);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forNode(ClusterNode node, ClusterNode... nodes) {
        return grid.forNode(node, nodes);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forOthers(ClusterNode node, ClusterNode... nodes) {
        return grid.forOthers(node, nodes);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forOthers(GridProjection prj) {
        return grid.forOthers(prj);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forNodeIds(Collection<UUID> ids) {
        return grid.forNodeIds(ids);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forNodeId(UUID id, UUID... ids) {
        return grid.forNodeId(id, ids);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forPredicate(GridPredicate<ClusterNode> p) {
        return grid.forPredicate(p);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forAttribute(String name, @Nullable String val) {
        return grid.forAttribute(name, val);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forCache(String cacheName, @Nullable String... cacheNames) {
        return grid.forCache(cacheName, cacheNames);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forStreamer(String streamerName, @Nullable String... streamerNames) {
        return grid.forStreamer(streamerName, streamerNames);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forRemotes() {
        return grid.forRemotes();
    }

    /** {@inheritDoc} */
    @Override public GridProjection forHost(ClusterNode node) {
        return grid.forHost(node);
    }

    /** {@inheritDoc} */
    @Override public GridProjection forDaemons() {
        return grid.forDaemons();
    }

    /** {@inheritDoc} */
    @Override public GridProjection forRandom() {
        return grid.forRandom();
    }

    /** {@inheritDoc} */
    @Override public GridProjection forOldest() {
        return grid.forOldest();
    }

    /** {@inheritDoc} */
    @Override public GridProjection forYoungest() {
        return grid.forYoungest();
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes() {
        return grid.nodes();
    }

    /** {@inheritDoc} */
    @Nullable @Override public ClusterNode node(UUID id) {
        return grid.node(id);
    }

    /** {@inheritDoc} */
    @Nullable @Override public ClusterNode node() {
        return grid.node();
    }

    /** {@inheritDoc} */
    @Override public GridPredicate<ClusterNode> predicate() {
        return grid.predicate();
    }

    /** {@inheritDoc} */
    @Override public ClusterMetrics metrics() throws GridException {
        return grid.metrics();
    }
}
