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

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.processors.query.h2.*;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.h2.command.*;
import org.h2.command.ddl.*;
import org.h2.command.dml.*;
import org.h2.engine.*;
import org.h2.expression.*;
import org.h2.index.*;
import org.h2.jdbc.*;
import org.h2.result.*;
import org.h2.table.*;
import org.h2.tools.*;
import org.h2.util.*;
import org.h2.value.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import javax.cache.*;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.*;

/**
 * Reduce query executor.
 */
public class GridReduceQueryExecutor {
    /** */
    private GridKernalContext ctx;

    /** */
    private IgniteH2Indexing h2;

    /** */
    private IgniteLogger log;

    /** */
    private final AtomicLong reqIdGen = new AtomicLong();

    /** */
    private final ConcurrentMap<Long, QueryRun> runs = new ConcurrentHashMap8<>();

    /** */
    private static ThreadLocal<GridMergeTable> curFunTbl = new ThreadLocal<>();

    /** */
    private static final Constructor<JdbcResultSet> CONSTRUCTOR;

    /**
     * Init constructor.
     */
    static {
        try {
            CONSTRUCTOR = JdbcResultSet.class.getDeclaredConstructor(
                JdbcConnection.class,
                JdbcStatement.class,
                ResultInterface.class,
                Integer.TYPE,
                Boolean.TYPE,
                Boolean.TYPE,
                Boolean.TYPE
            );

            CONSTRUCTOR.setAccessible(true);
        }
        catch (NoSuchMethodException e) {
            throw new IllegalStateException("Check H2 version in classpath.", e);
        }
    }

    /** */
    private final GridSpinBusyLock busyLock;

    /**
     * @param busyLock Busy lock.
     */
    public GridReduceQueryExecutor(GridSpinBusyLock busyLock) {
        this.busyLock = busyLock;
    }

    /**
     * @param ctx Context.
     * @param h2 H2 Indexing.
     * @throws IgniteCheckedException If failed.
     */
    public void start(final GridKernalContext ctx, final IgniteH2Indexing h2) throws IgniteCheckedException {
        this.ctx = ctx;
        this.h2 = h2;

        log = ctx.log(GridReduceQueryExecutor.class);

        ctx.io().addMessageListener(GridTopic.TOPIC_QUERY, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                if (!busyLock.enterBusy())
                    return;

                try {
                    GridReduceQueryExecutor.this.onMessage(nodeId, msg);
                }
                finally {
                    busyLock.leaveBusy();
                }
            }
        });

        ctx.event().addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(final Event evt) {
                UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

                for (QueryRun r : runs.values()) {
                    for (GridMergeTable tbl : r.tbls) {
                        if (tbl.getScanIndex(null).hasSource(nodeId)) {
                            // Will attempt to retry. If reduce query was started it will fail on next page fetching.
                            retry(r, h2.readyTopologyVersion(), nodeId);

                            break;
                        }
                    }
                }
            }
        }, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     */
    public void onMessage(UUID nodeId, Object msg) {
        try {
            assert msg != null;

            ClusterNode node = ctx.discovery().node(nodeId);

            if (node == null)
                return; // Node left, ignore.

            boolean processed = true;

            if (msg instanceof GridQueryNextPageResponse)
                onNextPage(node, (GridQueryNextPageResponse)msg);
            else if (msg instanceof GridQueryFailResponse)
                onFail(node, (GridQueryFailResponse)msg);
            else
                processed = false;

            if (processed && log.isDebugEnabled())
                log.debug("Processed response: " + nodeId + "->" + ctx.localNodeId() + " " + msg);
        }
        catch(Throwable th) {
            U.error(log, "Failed to process message: " + msg, th);
        }
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    private void onFail(ClusterNode node, GridQueryFailResponse msg) {
        QueryRun r = runs.get(msg.queryRequestId());

        fail(r, node.id(), msg.error());
    }

    /**
     * @param r Query run.
     * @param nodeId Failed node ID.
     * @param msg Error message.
     */
    private void fail(QueryRun r, UUID nodeId, String msg) {
        if (r != null)
            r.state(new CacheException("Failed to execute map query on the node: " + nodeId + ", " + msg), nodeId);
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    private void onNextPage(final ClusterNode node, GridQueryNextPageResponse msg) {
        final long qryReqId = msg.queryRequestId();
        final int qry = msg.query();

        final QueryRun r = runs.get(qryReqId);

        if (r == null) // Already finished with error or canceled.
            return;

        final int pageSize = r.pageSize;

        GridMergeIndex idx = r.tbls.get(msg.query()).getScanIndex(null);

        GridResultPage page;

        try {
            page = new GridResultPage(ctx, node.id(), msg) {
                @Override public void fetchNextPage() {
                    Object errState = r.state.get();

                    if (errState != null) {
                        CacheException err0 = errState instanceof CacheException ? (CacheException)errState : null;

                        if (err0 != null && err0.getCause() instanceof IgniteClientDisconnectedException)
                            throw err0;

                        CacheException e = new CacheException("Failed to fetch data from node: " + node.id());

                        if (err0 != null)
                            e.addSuppressed(err0);

                        throw e;
                    }

                    try {
                        GridQueryNextPageRequest msg0 = new GridQueryNextPageRequest(qryReqId, qry, pageSize);

                        if (node.isLocal())
                            h2.mapQueryExecutor().onMessage(ctx.localNodeId(), msg0);
                        else
                            ctx.io().send(node, GridTopic.TOPIC_QUERY, msg0, GridIoPolicy.PUBLIC_POOL);
                    }
                    catch (IgniteCheckedException e) {
                        throw new CacheException("Failed to fetch data from node: " + node.id(), e);
                    }
                }
            };
        }
        catch (Exception e) {
            U.error(log, "Error in message.", e);

            fail(r, node.id(), "Error in message.");

            return;
        }

        idx.addPage(page);

        if (msg.retry() != null)
            retry(r, msg.retry(), node.id());
        else if (msg.allRows() != -1) // Only the first page contains row count.
            r.latch.countDown();
    }

    /**
     * @param r Query run.
     * @param retryVer Retry version.
     * @param nodeId Node ID.
     */
    private void retry(QueryRun r, AffinityTopologyVersion retryVer, UUID nodeId) {
        r.state(retryVer, nodeId);
    }

    /**
     * @param cctx Cache context for main space.
     * @param extraSpaces Extra spaces.
     * @return {@code true} If preloading is active.
     */
    private boolean isPreloadingActive(final GridCacheContext<?,?> cctx, List<String> extraSpaces) {
        if (hasMovingPartitions(cctx))
            return true;

        if (extraSpaces != null) {
            for (String extraSpace : extraSpaces) {
                if (hasMovingPartitions(cacheContext(extraSpace)))
                    return true;
            }
        }

        return false;
    }

    /**
     * @param cctx Cache context.
     * @return {@code true} If cache context
     */
    private boolean hasMovingPartitions(GridCacheContext<?,?> cctx) {
        GridDhtPartitionFullMap fullMap = cctx.topology().partitionMap(false);

        for (GridDhtPartitionMap map : fullMap.values()) {
            if (map.hasMovingPartitions())
                return true;
        }

        return false;
    }

    /**
     * @param name Cache name.
     * @return Cache context.
     */
    private GridCacheContext<?,?> cacheContext(String name) {
        return ctx.cache().internalCache(name).context();
    }

    /**
     * @param topVer Topology version.
     * @param cctx Cache context for main space.
     * @param extraSpaces Extra spaces.
     * @return Data nodes or {@code null} if repartitioning started and we need to retry..
     */
    private Collection<ClusterNode> stableDataNodes(
        AffinityTopologyVersion topVer,
        final GridCacheContext<?,?> cctx,
        List<String> extraSpaces
    ) {
        String space = cctx.name();

        Set<ClusterNode> nodes = new HashSet<>(dataNodes(space, topVer));

        if (F.isEmpty(nodes))
            throw new CacheException("Failed to find data nodes for cache: " + space);

        if (!F.isEmpty(extraSpaces)) {
            for (String extraSpace : extraSpaces) {
                GridCacheContext<?,?> extraCctx = cacheContext(extraSpace);

                if (extraCctx.isLocal())
                    continue; // No consistency guaranties for local caches.

                if (cctx.isReplicated() && !extraCctx.isReplicated())
                    throw new CacheException("Queries running on replicated cache should not contain JOINs " +
                        "with partitioned tables [rCache=" + cctx.name() + ", pCache=" + extraSpace + "]");

                Collection<ClusterNode> extraNodes = dataNodes(extraSpace, topVer);

                if (F.isEmpty(extraNodes))
                    throw new CacheException("Failed to find data nodes for cache: " + extraSpace);

                if (cctx.isReplicated() && extraCctx.isReplicated()) {
                    nodes.retainAll(extraNodes);

                    if (nodes.isEmpty()) {
                        if (isPreloadingActive(cctx, extraSpaces))
                            return null; // Retry.
                        else
                            throw new CacheException("Caches have distinct sets of data nodes [cache1=" + cctx.name() +
                                ", cache2=" + extraSpace + "]");
                    }
                }
                else if (!cctx.isReplicated() && extraCctx.isReplicated()) {
                    if (!extraNodes.containsAll(nodes))
                        if (isPreloadingActive(cctx, extraSpaces))
                            return null; // Retry.
                        else
                            throw new CacheException("Caches have distinct sets of data nodes [cache1=" + cctx.name() +
                                ", cache2=" + extraSpace + "]");
                }
                else if (!cctx.isReplicated() && !extraCctx.isReplicated()) {
                    if (extraNodes.size() != nodes.size() || !nodes.containsAll(extraNodes))
                        if (isPreloadingActive(cctx, extraSpaces))
                            return null; // Retry.
                        else
                            throw new CacheException("Caches have distinct sets of data nodes [cache1=" + cctx.name() +
                                ", cache2=" + extraSpace + "]");
                }
                else
                    throw new IllegalStateException();
            }
        }

        return nodes;
    }

    /**
     * @param cctx Cache context.
     * @param qry Query.
     * @param keepPortable Keep portable.
     * @return Cursor.
     */
    public Iterator<List<?>> query(GridCacheContext<?,?> cctx, GridCacheTwoStepQuery qry, boolean keepPortable) {
        for (int attempt = 0;; attempt++) {
            if (attempt != 0) {
                try {
                    Thread.sleep(attempt * 10); // Wait for exchange.
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new CacheException("Query was interrupted.", e);
                }
            }

            long qryReqId = reqIdGen.incrementAndGet();

            QueryRun r = new QueryRun();

            r.pageSize = qry.pageSize() <= 0 ? GridCacheTwoStepQuery.DFLT_PAGE_SIZE : qry.pageSize();

            r.tbls = new ArrayList<>(qry.mapQueries().size());

            String space = cctx.name();

            r.conn = (JdbcConnection)h2.connectionForSpace(space);

            AffinityTopologyVersion topVer = h2.readyTopologyVersion();

            List<String> extraSpaces = extraSpaces(space, qry.spaces());

            Collection<ClusterNode> nodes;

            // Explicit partition mapping for unstable topology.
            Map<ClusterNode, IntArray> partsMap = null;

            if (isPreloadingActive(cctx, extraSpaces)) {
                if (cctx.isReplicated())
                    nodes = replicatedUnstableDataNodes(cctx, extraSpaces);
                else {
                    partsMap = partitionedUnstableDataNodes(cctx, extraSpaces);

                    nodes = partsMap == null ? null : partsMap.keySet();
                }
            }
            else
                nodes = stableDataNodes(topVer, cctx, extraSpaces);

            if (nodes == null)
                continue; // Retry.

            assert !nodes.isEmpty();

            if (cctx.isReplicated() || qry.explain()) {
                assert qry.explain() || !nodes.contains(ctx.cluster().get().localNode()) :
                    "We must be on a client node.";

                // Select random data node to run query on a replicated data or get EXPLAIN PLAN from a single node.
                nodes = Collections.singleton(F.rand(nodes));
            }

            for (GridCacheSqlQuery mapQry : qry.mapQueries()) {
                GridMergeTable tbl;

                try {
                    tbl = createFunctionTable(r.conn, mapQry, qry.explain()); // createTable(r.conn, mapQry); TODO
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }

                GridMergeIndex idx = tbl.getScanIndex(null);

                for (ClusterNode node : nodes)
                    idx.addSource(node.id());

                r.tbls.add(tbl);

                curFunTbl.set(tbl);
            }

            r.latch = new CountDownLatch(r.tbls.size() * nodes.size());

            runs.put(qryReqId, r);

            try {
                if (ctx.clientDisconnected()) {
                    throw new CacheException("Query was cancelled, client node disconnected.",
                        new IgniteClientDisconnectedException(ctx.cluster().clientReconnectFuture(),
                        "Client node disconnected."));
                }

                Collection<GridCacheSqlQuery> mapQrys = qry.mapQueries();

                if (qry.explain()) {
                    mapQrys = new ArrayList<>(qry.mapQueries().size());

                    for (GridCacheSqlQuery mapQry : qry.mapQueries())
                        mapQrys.add(new GridCacheSqlQuery(mapQry.alias(), "EXPLAIN " + mapQry.query(), mapQry.parameters()));
                }

                if (nodes.size() != 1 || !F.first(nodes).isLocal()) { // Marshall params for remotes.
                    Marshaller m = ctx.config().getMarshaller();

                    for (GridCacheSqlQuery mapQry : mapQrys)
                        mapQry.marshallParams(m);
                }

                boolean retry = false;

                if (send(nodes,
                    new GridQueryRequest(qryReqId, r.pageSize, space, mapQrys, topVer, extraSpaces, null), partsMap)) {
                    U.await(r.latch);

                    Object state = r.state.get();

                    if (state != null) {
                        if (state instanceof CacheException) {
                            CacheException err = (CacheException)state;

                            if (err.getCause() instanceof IgniteClientDisconnectedException)
                                throw err;

                            throw new CacheException("Failed to run map query remotely.", err);
                        }

                        if (state instanceof AffinityTopologyVersion) {
                            retry = true;

                            // If remote node asks us to retry then we have outdated full partition map.
                            h2.awaitForReadyTopologyVersion((AffinityTopologyVersion)state);
                        }
                    }
                }
                else // Send failed.
                    retry = true;

                ResultSet res = null;

                if (!retry) {
                    if (qry.explain())
                        return explainPlan(r.conn, space, qry);

                    GridCacheSqlQuery rdc = qry.reduceQuery();

                    res = h2.executeSqlQueryWithTimer(space, r.conn, rdc.query(), F.asList(rdc.parameters()));
                }

                for (GridMergeTable tbl : r.tbls) {
                    if (!tbl.getScanIndex(null).fetchedAll()) // We have to explicitly cancel queries on remote nodes.
                        send(nodes, new GridQueryCancelRequest(qryReqId), null);

//                dropTable(r.conn, tbl.getName()); TODO
                }

                if (retry) {
                    if (Thread.currentThread().isInterrupted())
                        throw new IgniteInterruptedCheckedException("Query was interrupted.");

                    continue;
                }

                return new GridQueryCacheObjectsIterator(new Iter(res), cctx, keepPortable);
            }
            catch (IgniteCheckedException | RuntimeException e) {
                U.closeQuiet(r.conn);

                if (e instanceof CacheException)
                    throw (CacheException)e;

                Throwable cause = e;

                if (e instanceof IgniteCheckedException) {
                    Throwable disconnectedErr =
                        ((IgniteCheckedException)e).getCause(IgniteClientDisconnectedException.class);

                    if (disconnectedErr != null)
                        cause = disconnectedErr;
                }

                throw new CacheException("Failed to run reduce query locally.", cause);
            }
            finally {
                if (!runs.remove(qryReqId, r))
                    U.warn(log, "Query run was already removed: " + qryReqId);

                curFunTbl.remove();
            }
        }
    }

    /**
     * Calculates data nodes for replicated caches on unstable topology.
     *
     * @param cctx Cache context for main space.
     * @param extraSpaces Extra spaces.
     * @return Collection of all data nodes owning all the caches or {@code null} for retry.
     */
    private Collection<ClusterNode> replicatedUnstableDataNodes(final GridCacheContext<?,?> cctx,
        List<String> extraSpaces) {
        assert cctx.isReplicated() : cctx.name() + " must be replicated";

        Set<ClusterNode> nodes = replicatedUnstableDataNodes(cctx);

        if (F.isEmpty(nodes))
            return null; // Retry.

        if (!F.isEmpty(extraSpaces)) {
            for (String extraSpace : extraSpaces) {
                GridCacheContext<?,?> extraCctx = cacheContext(extraSpace);

                if (extraCctx.isLocal())
                    continue;

                if (!extraCctx.isReplicated())
                    throw new CacheException("Queries running on replicated cache should not contain JOINs " +
                        "with tables in partitioned caches [rCache=" + cctx.name() + ", pCache=" + extraSpace + "]");

                Set<ClusterNode> extraOwners = replicatedUnstableDataNodes(extraCctx);

                if (F.isEmpty(extraOwners))
                    return null; // Retry.

                nodes.retainAll(extraOwners);

                if (nodes.isEmpty())
                    return null; // Retry.
            }
        }

        return nodes;
    }

    /**
     * @param space Cache name.
     * @param topVer Topology version.
     * @return Collection of data nodes.
     */
    private Collection<ClusterNode> dataNodes(String space, AffinityTopologyVersion topVer) {
        Collection<ClusterNode> res = ctx.discovery().cacheAffinityNodes(space, topVer);

        return res != null ? res : Collections.<ClusterNode>emptySet();
    }

    /**
     * Collects all the nodes owning all the partitions for the given replicated cache.
     *
     * @param cctx Cache context.
     * @return Owning nodes or {@code null} if we can't find owners for some partitions.
     */
    private Set<ClusterNode> replicatedUnstableDataNodes(GridCacheContext<?,?> cctx) {
        assert cctx.isReplicated() : cctx.name() + " must be replicated";

        String space = cctx.name();

        Set<ClusterNode> dataNodes = new HashSet<>(dataNodes(space, NONE));

        if (dataNodes.isEmpty())
            throw new CacheException("Failed to find data nodes for cache: " + space);

        // Find all the nodes owning all the partitions for replicated cache.
        for (int p = 0, parts = cctx.affinity().partitions(); p < parts; p++) {
            List<ClusterNode> owners = cctx.topology().owners(p);

            if (F.isEmpty(owners))
                return null; // Retry.

            dataNodes.retainAll(owners);

            if (dataNodes.isEmpty())
                return null; // Retry.
        }

        return dataNodes;
    }

    /**
     * Calculates partition mapping for partitioned cache on unstable topology.
     *
     * @param cctx Cache context for main space.
     * @param extraSpaces Extra spaces.
     * @return Partition mapping or {@code null} if we can't calculate it due to repartitioning and we need to retry.
     */
    @SuppressWarnings("unchecked")
    private Map<ClusterNode, IntArray> partitionedUnstableDataNodes(final GridCacheContext<?,?> cctx,
        List<String> extraSpaces) {
        assert !cctx.isReplicated() && !cctx.isLocal() : cctx.name() + " must be partitioned";

        final int partsCnt = cctx.affinity().partitions();

        if (extraSpaces != null) { // Check correct number of partitions for partitioned caches.
            for (String extraSpace : extraSpaces) {
                GridCacheContext<?,?> extraCctx = cacheContext(extraSpace);

                if (extraCctx.isReplicated() || extraCctx.isLocal())
                    continue;

                int parts = extraCctx.affinity().partitions();

                if (parts != partsCnt)
                    throw new CacheException("Number of partitions must be the same for correct collocation [cache1=" +
                        cctx.name() + ", parts1=" + partsCnt + ", cache2=" + extraSpace + ", parts2=" + parts + "]");
            }
        }

        Set<ClusterNode>[] partLocs = new Set[partsCnt];

        // Fill partition locations for main cache.
        for (int p = 0, parts =  cctx.affinity().partitions(); p < parts; p++) {
            List<ClusterNode> owners = cctx.topology().owners(p);

            if (F.isEmpty(owners)) {
                if (!F.isEmpty(dataNodes(cctx.name(), NONE)))
                    return null; // Retry.

                throw new CacheException("Failed to find data nodes [cache=" + cctx.name() + ", part=" + p + "]");
            }

            partLocs[p] = new HashSet<>(owners);
        }

        if (extraSpaces != null) {
            // Find owner intersections for each participating partitioned cache partition.
            // We need this for logical collocation between different partitioned caches with the same affinity.
            for (String extraSpace : extraSpaces) {
                GridCacheContext<?,?> extraCctx = cacheContext(extraSpace);

                if (extraCctx.isReplicated() || extraCctx.isLocal())
                    continue;

                for (int p = 0, parts =  extraCctx.affinity().partitions(); p < parts; p++) {
                    List<ClusterNode> owners = extraCctx.topology().owners(p);

                    if (F.isEmpty(owners)) {
                        if (!F.isEmpty(dataNodes(extraSpace, NONE)))
                            return null; // Retry.

                        throw new CacheException("Failed to find data nodes [cache=" + extraSpace + ", part=" + p + "]");
                    }

                    if (partLocs[p] == null)
                        partLocs[p] = new HashSet<>(owners);
                    else {
                        partLocs[p].retainAll(owners); // Intersection of owners.

                        if (partLocs[p].isEmpty())
                            return null; // Intersection is empty -> retry.
                    }
                }
            }

            // Filter nodes where not all the replicated caches loaded.
            for (String extraSpace : extraSpaces) {
                GridCacheContext<?,?> extraCctx = cacheContext(extraSpace);

                if (!extraCctx.isReplicated())
                    continue;

                Set<ClusterNode> dataNodes = replicatedUnstableDataNodes(extraCctx);

                if (F.isEmpty(dataNodes))
                    return null; // Retry.

                for (Set<ClusterNode> partLoc : partLocs) {
                    partLoc.retainAll(dataNodes);

                    if (partLoc.isEmpty())
                        return null; // Retry.
                }
            }
        }

        // Collect the final partitions mapping.
        Map<ClusterNode, IntArray> res = new HashMap<>();

        // Here partitions in all IntArray's will be sorted in ascending order, this is important.
        for (int p = 0; p < partLocs.length; p++) {
            Set<ClusterNode> pl = partLocs[p];

            assert !F.isEmpty(pl) : pl;

            ClusterNode n = pl.size() == 1 ? F.first(pl) : F.rand(pl);

            IntArray parts = res.get(n);

            if (parts == null)
                res.put(n, parts = new IntArray());

            parts.add(p);
        }

        return res;
    }

    /**
     * @param mainSpace Main space.
     * @param allSpaces All spaces.
     * @return List of all extra spaces or {@code null} if none.
     */
    private List<String> extraSpaces(String mainSpace, Set<String> allSpaces) {
        if (F.isEmpty(allSpaces) || (allSpaces.size() == 1 && allSpaces.contains(mainSpace)))
            return null;

        ArrayList<String> res = new ArrayList<>(allSpaces.size());

        for (String space : allSpaces) {
            if (!F.eq(space, mainSpace))
                res.add(space);
        }

        return res;
    }

    /**
     * @param c Connection.
     * @param space Space.
     * @param qry Query.
     * @return Cursor for plans.
     * @throws IgniteCheckedException if failed.
     */
    private Iterator<List<?>> explainPlan(JdbcConnection c, String space, GridCacheTwoStepQuery qry)
        throws IgniteCheckedException {
        List<List<?>> lists = new ArrayList<>();

        for (GridCacheSqlQuery mapQry : qry.mapQueries()) {
            ResultSet rs = h2.executeSqlQueryWithTimer(space, c, "SELECT PLAN FROM " + mapQry.alias(), null);

            lists.add(F.asList(getPlan(rs)));
        }

        for (GridCacheSqlQuery mapQry : qry.mapQueries()) {
            GridMergeTable tbl = createFunctionTable(c, mapQry, false);

            curFunTbl.set(tbl); // Now it will be only a single table.
        }

        GridCacheSqlQuery rdc = qry.reduceQuery();

        ResultSet rs = h2.executeSqlQueryWithTimer(space, c, "EXPLAIN " + rdc.query(), F.asList(rdc.parameters()));

        lists.add(F.asList(getPlan(rs)));

        return lists.iterator();
    }

    /**
     * @param rs Result set.
     * @return Plan.
     * @throws IgniteCheckedException If failed.
     */
    private String getPlan(ResultSet rs) throws IgniteCheckedException {
        try {
            if (!rs.next())
                throw new IllegalStateException();

            return rs.getString(1);
        }
        catch (SQLException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param nodes Nodes.
     * @param msg Message.
     * @param partsMap Partitions.
     * @return {@code true} If all messages sent successfully.
     */
    private boolean send(
        Collection<ClusterNode> nodes,
        Message msg,
        Map<ClusterNode,IntArray> partsMap
    ) {
        boolean locNodeFound = false;

        boolean ok = true;

        for (ClusterNode node : nodes) {
            if (node.isLocal()) {
                locNodeFound = true;

                continue;
            }

            try {
                ctx.io().send(node, GridTopic.TOPIC_QUERY, copy(msg, node, partsMap), GridIoPolicy.PUBLIC_POOL);
            }
            catch (IgniteCheckedException e) {
                ok = false;

                U.warn(log, e.getMessage());
            }
        }

        if (locNodeFound) // Local node goes the last to allow parallel execution.
            h2.mapQueryExecutor().onMessage(ctx.localNodeId(), copy(msg, ctx.cluster().get().localNode(), partsMap));

        return ok;
    }

    /**
     * @param msg Message to copy.
     * @param node Node.
     * @param partsMap Partitions map.
     * @return Copy of message with partitions set.
     */
    private Message copy(Message msg, ClusterNode node, Map<ClusterNode,IntArray> partsMap) {
        if (partsMap == null)
            return msg;

        GridQueryRequest res = new GridQueryRequest((GridQueryRequest)msg);

        IntArray parts = partsMap.get(node);

        assert parts != null : node;

        int[] partsArr = new int[parts.size()];

        parts.toArray(partsArr);

        res.partitions(partsArr);

        return res;
    }

    /**
     * @param conn Connection.
     * @param tblName Table name.
     * @throws SQLException If failed.
     */
    private void dropTable(Connection conn, String tblName) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE " + tblName);
        }
    }

    /**
     * @return Merged result set.
     */
    public static ResultSet mergeTableFunction(JdbcConnection c) throws Exception {
        GridMergeTable tbl = curFunTbl.get();

        Session ses = (Session)c.getSession();

        String url = c.getMetaData().getURL();

        // URL is either "jdbc:default:connection" or "jdbc:columnlist:connection"
        final Cursor cursor = url.charAt(5) == 'c' ? null : tbl.getScanIndex(ses).find(ses, null, null);

        final Column[] cols = tbl.getColumns();

        SimpleResultSet rs = new SimpleResultSet(cursor == null ? null : new SimpleRowSource() {
            @Override public Object[] readRow() throws SQLException {
                if (!cursor.next())
                    return null;

                Row r = cursor.get();

                Object[] row = new Object[cols.length];

                for (int i = 0; i < row.length; i++)
                    row[i] = r.getValue(i).getObject();

                return row;
            }

            @Override public void close() {
                // No-op.
            }

            @Override public void reset() throws SQLException {
                throw new SQLException("Unsupported.");
            }
        }) {
            @Override public byte[] getBytes(int colIdx) throws SQLException {
                assert cursor != null;

                return cursor.get().getValue(colIdx - 1).getBytes();
            }

            @Override public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
                throw new UnsupportedOperationException();
            }

            @Override public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
                throw new UnsupportedOperationException();
            }
        };

        for (Column col : cols)
            rs.addColumn(col.getName(), DataType.convertTypeToSQLType(col.getType()),
                MathUtils.convertLongToInt(col.getPrecision()), col.getScale());

        return rs;
    }

    /**
     * @param asQuery Query.
     * @return List of columns.
     */
    private static ArrayList<Column> generateColumnsFromQuery(org.h2.command.dml.Query asQuery) {
        int columnCount = asQuery.getColumnCount();
        ArrayList<Expression> expressions = asQuery.getExpressions();
        ArrayList<Column> cols = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            Expression expr = expressions.get(i);
            int type = expr.getType();
            String name = expr.getAlias();
            long precision = expr.getPrecision();
            int displaySize = expr.getDisplaySize();
            DataType dt = DataType.getDataType(type);
            if (precision > 0 && (dt.defaultPrecision == 0 ||
                (dt.defaultPrecision > precision && dt.defaultPrecision < Byte.MAX_VALUE))) {
                // dont' set precision to MAX_VALUE if this is the default
                precision = dt.defaultPrecision;
            }
            int scale = expr.getScale();
            if (scale > 0 && (dt.defaultScale == 0 ||
                (dt.defaultScale > scale && dt.defaultScale < precision))) {
                scale = dt.defaultScale;
            }
            if (scale > precision) {
                precision = scale;
            }
            Column col = new Column(name, type, precision, scale, displaySize);
            cols.add(col);
        }

        return cols;
    }

    /**
     * @param conn Connection.
     * @param qry Query.
     * @param explain Explain.
     * @return Table.
     * @throws IgniteCheckedException
     */
    private GridMergeTable createFunctionTable(JdbcConnection conn, GridCacheSqlQuery qry, boolean explain)
        throws IgniteCheckedException {
        try {
            Session ses = (Session)conn.getSession();

            CreateTableData data  = new CreateTableData();

            data.tableName = "T___";
            data.schema = ses.getDatabase().getSchema(ses.getCurrentSchemaName());
            data.create = true;

            if (!explain) {
                Prepared prepare = ses.prepare(qry.query(), false);

                List<org.h2.expression.Parameter> parsedParams = prepare.getParameters();

                for (int i = Math.min(parsedParams.size(), qry.parameters().length); --i >= 0; ) {
                    Object val = qry.parameters()[i];

                    parsedParams.get(i).setValue(DataType.convertToValue(ses, val, Value.UNKNOWN));
                }

                data.columns = generateColumnsFromQuery((Query)prepare);
            }
            else
                data.columns = planColumns();

            return new GridMergeTable(data);
        }
        catch (Exception e) {
            U.closeQuiet(conn);

            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @return Columns.
     */
    private static ArrayList<Column> planColumns() {
        ArrayList<Column> res = new ArrayList<>(1);

        res.add(new Column("PLAN", Value.STRING));

        return res;
    }

    /**
     * @param conn Connection.
     * @param qry Query.
     * @return Table.
     * @throws IgniteCheckedException If failed.
     */
    private GridMergeTable createTable(Connection conn, GridCacheSqlQuery qry) throws IgniteCheckedException {
        try {
            try (PreparedStatement s = conn.prepareStatement(
                "CREATE LOCAL TEMPORARY TABLE " + qry.alias() +
                " ENGINE \"" + GridMergeTable.Engine.class.getName() + "\" " +
                " AS SELECT * FROM (" + qry.query() + ") WHERE FALSE")) {
                h2.bindParameters(s, F.asList(qry.parameters()));

                s.execute();
            }

            return GridMergeTable.Engine.getCreated();
        }
        catch (SQLException e) {
            U.closeQuiet(conn);

            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param reconnectFut Reconnect future.
     */
    public void onDisconnected(IgniteFuture<?> reconnectFut) {
        CacheException err = new CacheException("Query was cancelled, client node disconnected.",
            new IgniteClientDisconnectedException(reconnectFut, "Client node disconnected."));

        for (Map.Entry<Long, QueryRun> e : runs.entrySet())
            e.getValue().disconnected(err);
    }

    /**
     *
     */
    private static class QueryRun {
        /** */
        private List<GridMergeTable> tbls;

        /** */
        private CountDownLatch latch;

        /** */
        private JdbcConnection conn;

        /** */
        private int pageSize;

        /** Can be either CacheException in case of error or AffinityTopologyVersion to retry if needed. */
        private final AtomicReference<Object> state = new AtomicReference<>();

        /**
         * @param o Fail state object.
         * @param nodeId Node ID.
         */
        void state(Object o, @Nullable UUID nodeId) {
            assert o != null;
            assert o instanceof CacheException || o instanceof AffinityTopologyVersion : o.getClass();

            if (!state.compareAndSet(null, o))
                return;

            while (latch.getCount() != 0) // We don't need to wait for all nodes to reply.
                latch.countDown();

            for (GridMergeTable tbl : tbls) // Fail all merge indexes.
                tbl.getScanIndex(null).fail(nodeId);
        }

        /**
         * @param e Error.
         */
        void disconnected(CacheException e) {
            if (!state.compareAndSet(null, e))
                return;

            while (latch.getCount() != 0) // We don't need to wait for all nodes to reply.
                latch.countDown();

            for (GridMergeTable tbl : tbls) // Fail all merge indexes.
                tbl.getScanIndex(null).fail(e);
        }
    }

    /**
     *
     */
    private static class Iter extends GridH2ResultSetIterator<List<?>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param data Data array.
         * @throws IgniteCheckedException If failed.
         */
        protected Iter(ResultSet data) throws IgniteCheckedException {
            super(data);
        }

        /** {@inheritDoc} */
        @Override protected List<?> createRow() {
            ArrayList<Object> res = new ArrayList<>(row.length);

            Collections.addAll(res, row);

            return res;
        }
    }
}
