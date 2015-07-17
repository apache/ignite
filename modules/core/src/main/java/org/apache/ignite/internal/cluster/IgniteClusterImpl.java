/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cluster;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.nodestart.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.IgniteNodeAttributes.*;
import static org.apache.ignite.internal.util.nodestart.IgniteNodeStartUtils.*;

/**
 *
 */
public class IgniteClusterImpl extends ClusterGroupAdapter implements IgniteClusterEx, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteConfiguration cfg;

    /** Node local store. */
    @GridToStringExclude
    private ConcurrentMap nodeLoc;

    /** Client reconnect future. */
    private IgniteFuture<?> reconnecFut;

    /**
     * Required by {@link Externalizable}.
     */
    public IgniteClusterImpl() {
        // No-op.
    }

    /**
     * @param ctx Kernal context.
     */
    public IgniteClusterImpl(GridKernalContext ctx) {
        super(ctx, null, (IgnitePredicate<ClusterNode>)null);

        cfg = ctx.config();

        nodeLoc = new ClusterNodeLocalMapImpl(ctx);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forLocal() {
        guard();

        try {
            return new ClusterGroupAdapter(ctx, null, Collections.singleton(cfg.getNodeId()));
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
        guard();

        try {
            ClusterNode node = ctx.discovery().localNode();

            assert node != null;

            return node;
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> ConcurrentMap<K, V> nodeLocalMap() {
        guard();

        try {
            return nodeLoc;
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        A.notNull(nodeId, "nodeId");

        guard();

        try {
            return ctx.discovery().pingNode(nodeId);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public long topologyVersion() {
        guard();

        try {
            return ctx.discovery().topologyVersion();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> topology(long topVer) throws UnsupportedOperationException {
        guard();

        try {
            return ctx.discovery().topology(topVer);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K> Map<ClusterNode, Collection<K>> mapKeysToNodes(@Nullable String cacheName,
        @Nullable Collection<? extends K> keys)
        throws IgniteException
    {
        if (F.isEmpty(keys))
            return Collections.emptyMap();

        guard();

        try {
            return ctx.affinity().mapKeysToNodes(cacheName, keys);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public <K> ClusterNode mapKeyToNode(@Nullable String cacheName, K key) throws IgniteException {
        A.notNull(key, "key");

        guard();

        try {
            return ctx.affinity().mapKeyToNode(cacheName, key);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterStartNodeResult> startNodes(File file,
        boolean restart,
        int timeout,
        int maxConn)
        throws IgniteException
    {
        try {
            return startNodesAsync(file, restart, timeout, maxConn).get();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterStartNodeResult> startNodes(Collection<Map<String, Object>> hosts,
        @Nullable Map<String, Object> dflts,
        boolean restart,
        int timeout,
        int maxConn)
        throws IgniteException
    {
        try {
            return startNodesAsync(hosts, dflts, restart, timeout, maxConn).get();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void stopNodes() throws IgniteException {
        guard();

        try {
            compute().execute(IgniteKillTask.class, false);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void stopNodes(Collection<UUID> ids) throws IgniteException {
        guard();

        try {
            ctx.grid().compute(forNodeIds(ids)).execute(IgniteKillTask.class, false);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void restartNodes() throws IgniteException {
        guard();

        try {
            compute().execute(IgniteKillTask.class, true);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void restartNodes(Collection<UUID> ids) throws IgniteException {
        guard();

        try {
            ctx.grid().compute(forNodeIds(ids)).execute(IgniteKillTask.class, true);
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        guard();

        try {
            ctx.jobMetric().reset();
            ctx.io().resetMetrics();
            ctx.task().resetMetrics();
        }
        finally {
            unguard();
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteCluster withAsync() {
        return new IgniteClusterAsyncImpl(this);
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        throw new IllegalStateException("Asynchronous mode is not enabled.");
    }

    /**
     * @param file Configuration file.
     * @param restart Whether to stop existing nodes.
     * @param timeout Connection timeout.
     * @param maxConn Number of parallel SSH connections to one host.
     * @return Future with results.
     * @see IgniteCluster#startNodes(java.io.File, boolean, int, int)
     */
    IgniteInternalFuture<Collection<ClusterStartNodeResult>> startNodesAsync(File file,
      boolean restart,
      int timeout,
      int maxConn)
    {
        A.notNull(file, "file");
        A.ensure(file.exists(), "file doesn't exist.");
        A.ensure(file.isFile(), "file is a directory.");

        try {
            IgniteBiTuple<Collection<Map<String, Object>>, Map<String, Object>> t = parseFile(file);

            return startNodesAsync(t.get1(), t.get2(), restart, timeout, maxConn);
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * @param hosts Startup parameters.
     * @param dflts Default values.
     * @param restart Whether to stop existing nodes
     * @param timeout Connection timeout in milliseconds.
     * @param maxConn Number of parallel SSH connections to one host.
     * @return Future with results.
     * @see IgniteCluster#startNodes(java.util.Collection, java.util.Map, boolean, int, int)
     */
    IgniteInternalFuture<Collection<ClusterStartNodeResult>> startNodesAsync(
        Collection<Map<String, Object>> hosts,
        @Nullable Map<String, Object> dflts,
        boolean restart,
        int timeout,
        int maxConn)
    {
        A.notNull(hosts, "hosts");

        guard();

        try {
            IgniteSshHelper sshHelper = IgniteComponentType.SSH.create(false);

            Map<String, Collection<IgniteRemoteStartSpecification>> specsMap = specifications(hosts, dflts);

            Map<String, ConcurrentLinkedQueue<StartNodeCallable>> runMap = new HashMap<>();

            int nodeCallCnt = 0;

            for (String host : specsMap.keySet()) {
                InetAddress addr;

                try {
                    addr = InetAddress.getByName(host);
                }
                catch (UnknownHostException e) {
                    throw new IgniteCheckedException("Invalid host name: " + host, e);
                }

                Collection<? extends ClusterNode> neighbors = null;

                if (addr.isLoopbackAddress())
                    neighbors = neighbors();
                else {
                    for (Collection<ClusterNode> p : U.neighborhood(nodes()).values()) {
                        ClusterNode node = F.first(p);

                        if (node.<String>attribute(ATTR_IPS).contains(addr.getHostAddress())) {
                            neighbors = p;

                            break;
                        }
                    }
                }

                int startIdx = 1;

                if (neighbors != null) {
                    if (restart && !neighbors.isEmpty()) {
                        try {
                            ctx.grid().compute(forNodes(neighbors)).execute(IgniteKillTask.class, false);
                        }
                        catch (ClusterGroupEmptyException ignored) {
                            // No-op, nothing to restart.
                        }
                    }
                    else
                        startIdx = neighbors.size() + 1;
                }

                ConcurrentLinkedQueue<StartNodeCallable> nodeRuns = new ConcurrentLinkedQueue<>();

                runMap.put(host, nodeRuns);

                for (IgniteRemoteStartSpecification spec : specsMap.get(host)) {
                    assert spec.host().equals(host);

                    for (int i = startIdx; i <= spec.nodes(); i++) {
                        nodeRuns.add(sshHelper.nodeStartCallable(spec, timeout));

                        nodeCallCnt++;
                    }
                }
            }

            // If there is nothing to start, return finished future with empty result.
            if (nodeCallCnt == 0)
                return new GridFinishedFuture<Collection<ClusterStartNodeResult>>(
                    Collections.<ClusterStartNodeResult>emptyList());

            // Exceeding max line width for readability.
            GridCompoundFuture<ClusterStartNodeResult, Collection<ClusterStartNodeResult>> fut = 
                new GridCompoundFuture<>(CU.<ClusterStartNodeResult>objectsReducer());

            AtomicInteger cnt = new AtomicInteger(nodeCallCnt);

            // Limit maximum simultaneous connection number per host.
            for (ConcurrentLinkedQueue<StartNodeCallable> queue : runMap.values()) {
                for (int i = 0; i < maxConn; i++) {
                    if (!runNextNodeCallable(queue, fut, cnt))
                        break;
                }
            }

            return fut;
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
        }
        finally {
            unguard();
        }
    }

    /**
     * Gets the all grid nodes that reside on the same physical computer as local grid node.
     * Local grid node is excluded.
     * <p>
     * Detection of the same physical computer is based on comparing set of network interface MACs.
     * If two nodes have the same set of MACs, Ignite considers these nodes running on the same
     * physical computer.
     * @return Grid nodes that reside on the same physical computer as local grid node.
     */
    private Collection<ClusterNode> neighbors() {
        Collection<ClusterNode> neighbors = new ArrayList<>(1);

        String macs = localNode().attribute(ATTR_MACS);

        assert macs != null;

        for (ClusterNode n : forOthers(localNode()).nodes()) {
            if (macs.equals(n.attribute(ATTR_MACS)))
                neighbors.add(n);
        }

        return neighbors;
    }

    /**
     * Runs next callable from host node start queue.
     *
     * @param queue Queue of tasks to poll from.
     * @param comp Compound future that comprise all started node tasks.
     * @param cnt Atomic counter to check if all futures are added to compound future.
     * @return {@code True} if task was started, {@code false} if queue was empty.
     */
    private boolean runNextNodeCallable(final ConcurrentLinkedQueue<StartNodeCallable> queue,
        final GridCompoundFuture<ClusterStartNodeResult, Collection<ClusterStartNodeResult>>
        comp,
        final AtomicInteger cnt)
    {
        StartNodeCallable call = queue.poll();

        if (call == null)
            return false;

        IgniteInternalFuture<ClusterStartNodeResult> fut = ctx.closure().callLocalSafe(call, true);

        comp.add(fut);

        if (cnt.decrementAndGet() == 0)
            comp.markInitialized();

        fut.listen(new CI1<IgniteInternalFuture<ClusterStartNodeResult>>() {
            @Override public void apply(IgniteInternalFuture<ClusterStartNodeResult> f) {
                runNextNodeCallable(queue, comp, cnt);
            }
        });

        return true;
    }

    /**
     * Clears node local map.
     */
    public void clearNodeMap() {
        nodeLoc.clear();
    }

    /**
     * @param reconnecFut Reconnect future.
     */
    public void clientReconnectFuture(IgniteFuture<?> reconnecFut) {
        this.reconnecFut = reconnecFut;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFuture<?> clientReconnectFuture() {
        return reconnecFut;
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ctx = (GridKernalContext)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ctx);
    }

    /** {@inheritDoc} */
    @Override protected Object readResolve() throws ObjectStreamException {
        return ctx.grid().cluster();
    }

    /** {@inheritDoc} */
    public String toString() {
        return "IgniteCluster [igniteName=" + ctx.gridName() + ']';
    }
}
