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

package org.apache.ignite.internal.processors.cache.query;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.INDEX;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SCAN;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SET;

/**
 * Query request.
 */
public class GridCacheQueryRequest extends GridCacheIdMessage implements GridCacheDeployable {
    /** */
    private static final int FLAG_DATA_PAGE_SCAN_DFLT = 0b00;

    /** */
    private static final int FLAG_DATA_PAGE_SCAN_ENABLED = 0b01;

    /** */
    private static final int FLAG_DATA_PAGE_SCAN_DISABLED = 0b10;

    /** */
    private static final int FLAG_DATA_PAGE_SCAN_MASK = 0b11;

    /** */
    @Order(4)
    long id;

    /** */
    @Order(5)
    String cacheName;

    /** */
    @Order(6)
    GridCacheQueryType type;

    /** */
    @Order(7)
    @GridToStringInclude(sensitive = true)
    String clause;

    /** */
    private IndexQueryDesc idxQryDesc;

    /** */
    @Order(8)
    byte[] idxQryDescBytes;

    /** */
    @Order(9)
    int limit;

    /** */
    @Order(10)
    String clsName;

    /** */
    private IgniteBiPredicate<Object, Object> keyValFilter;

    /** */
    @Order(11)
    byte[] keyValFilterBytes;

    /** */
    private IgniteReducer<Object, Object> rdc;

    /** */
    @Order(12)
    byte[] rdcBytes;

    /** */
    private IgniteClosure<?, ?> trans;

    /** */
    @Order(13)
    byte[] transBytes;

    /** */
    private Object[] args;

    /** */
    @Order(14)
    byte[] argsBytes;

    /** */
    @Order(15)
    int pageSize;

    /** */
    @Order(16)
    boolean incBackups;

    /** */
    @Order(17)
    boolean cancel;

    /** */
    @Order(18)
    boolean incMeta;

    /** */
    @Order(19)
    boolean all;

    /** */
    @Order(20)
    boolean keepBinary;

    /** */
    @Order(21)
    int taskHash;

    /** Partition. */
    @Order(22)
    int part = -1;

    /** */
    @Order(value = 23, method = "topologyVersion")
    AffinityTopologyVersion topVer;

    /** Set of keys that must be skiped during iteration. */
    @Order(24)
    Collection<KeyCacheObject> skipKeys;

    /** */
    @Order(25)
    byte flags;

    /**
     * Empty constructor.
     */
    public GridCacheQueryRequest() {
        // No-op.
    }

    /**
     * Send initial query request to specified nodes.
     *
     * @param cctx Cache context.
     * @param reqId Request (cache query) ID.
     * @param fut Cache query future, contains query info.
     */
    static GridCacheQueryRequest startQueryRequest(GridCacheContext<?, ?> cctx, long reqId,
        GridCacheDistributedQueryFuture<?, ?, ?> fut) {
        GridCacheQueryBean bean = fut.query();
        CacheQuery<?> qry = bean.query();

        boolean deployFilterOrTransformer = (qry.scanFilter() != null || qry.transform() != null)
            && cctx.gridDeploy().enabled();

        return new GridCacheQueryRequest(
            cctx.cacheId(),
            reqId,
            cctx.name(),
            qry.type(),
            fut.fields(),
            qry.clause(),
            qry.idxQryDesc(),
            qry.limit(),
            qry.queryClassName(),
            qry.scanFilter(),
            qry.partition(),
            bean.reducer(),
            qry.transform(),
            qry.pageSize(),
            qry.includeBackups(),
            bean.arguments(),
            qry.includeMetadata(),
            qry.keepBinary(),
            qry.taskHash(),
            cctx.affinity().affinityTopologyVersion(),
            // Force deployment anyway if scan query is used.
            cctx.deploymentEnabled() || deployFilterOrTransformer,
            qry.isDataPageScanEnabled(),
            qry.skipKeys());
    }

    /**
     * Send request for fetching query result pages to specified nodes.
     *
     * @param cctx Cache context.
     * @param reqId Request (cache query) ID.
     * @param qry Query.
     */
    static GridCacheQueryRequest pageRequest(GridCacheContext<?, ?> cctx, long reqId, CacheQuery<?> qry) {
        return new GridCacheQueryRequest(
            cctx.cacheId(),
            reqId,
            cctx.name(),
            qry.pageSize(),
            qry.includeBackups(),
            false,
            qry.keepBinary(),
            qry.taskHash(),
            cctx.affinity().affinityTopologyVersion(),
            // Force deployment anyway if scan query is used.
            cctx.deploymentEnabled() || (qry.scanFilter() != null && cctx.gridDeploy().enabled()),
            qry.isDataPageScanEnabled());
    }

    /**
     * Send cancel query request, so no new pages will be sent.
     *
     * @param cctx Cache context.
     * @param reqId Query request ID.
     */
    static GridCacheQueryRequest cancelRequest(GridCacheContext<?, ?> cctx, long reqId) {
        return new GridCacheQueryRequest(cctx.cacheId(),
            reqId,
            cctx.affinity().affinityTopologyVersion(),
            cctx.deploymentEnabled());
    }

    /**
     * Creates cancel query request.
     *
     * @param cacheId Cache ID.
     * @param id Request to cancel.
     * @param topVer Topology version.
     * @param addDepInfo Deployment info flag.
     */
    private GridCacheQueryRequest(
        int cacheId,
        long id,
        AffinityTopologyVersion topVer,
        boolean addDepInfo
    ) {
        this.cacheId = cacheId;
        this.id = id;
        this.topVer = topVer;
        this.addDepInfo = addDepInfo;

        cancel = true;
    }

    /**
     * Request to load page.
     *
     * @param cacheId Cache ID.
     * @param id Request ID.
     * @param cacheName Cache name.
     * @param pageSize Page size.
     * @param incBackups {@code true} if need to include backups.
     * @param all Whether to load all pages.
     * @param keepBinary Whether to keep binary.
     * @param taskHash Task name hash code.
     * @param topVer Topology version.
     * @param addDepInfo Deployment info flag.
     * @param dataPageScanEnabled Flag to enable data page scan.
     */
    private GridCacheQueryRequest(
        int cacheId,
        long id,
        String cacheName,
        int pageSize,
        boolean incBackups,
        boolean all,
        boolean keepBinary,
        int taskHash,
        AffinityTopologyVersion topVer,
        boolean addDepInfo,
        Boolean dataPageScanEnabled
    ) {
        this.cacheId = cacheId;
        this.id = id;
        this.cacheName = cacheName;
        this.pageSize = pageSize;
        this.incBackups = incBackups;
        this.all = all;
        this.keepBinary = keepBinary;
        this.taskHash = taskHash;
        this.topVer = topVer;
        this.addDepInfo = addDepInfo;

        flags = setDataPageScanEnabled(flags, dataPageScanEnabled);
    }

    /**
     * Request to start query.
     *
     * @param cacheId Cache ID.
     * @param id Request id.
     * @param cacheName Cache name.
     * @param type Query type.
     * @param fields {@code true} if query returns fields.
     * @param clause Query clause.
     * @param limit Response limit. Set to 0 for no limits.
     * @param clsName Query class name.
     * @param keyValFilter Key-value filter.
     * @param part Partition.
     * @param rdc Reducer.
     * @param trans Transformer.
     * @param pageSize Page size.
     * @param incBackups {@code true} if need to include backups.
     * @param args Query arguments.
     * @param incMeta Include meta data or not.
     * @param keepBinary Keep binary flag.
     * @param taskHash Task name hash code.
     * @param topVer Topology version.
     * @param addDepInfo Deployment info flag.
     * @param skipKeys Set of keys that must be skiped during iteration.
     */
    private GridCacheQueryRequest(
        int cacheId,
        long id,
        String cacheName,
        GridCacheQueryType type,
        boolean fields,
        String clause,
        IndexQueryDesc idxQryDesc,
        int limit,
        String clsName,
        IgniteBiPredicate<Object, Object> keyValFilter,
        @Nullable Integer part,
        IgniteReducer<Object, Object> rdc,
        IgniteClosure<?, ?> trans,
        int pageSize,
        boolean incBackups,
        Object[] args,
        boolean incMeta,
        boolean keepBinary,
        int taskHash,
        AffinityTopologyVersion topVer,
        boolean addDepInfo,
        Boolean dataPageScanEnabled,
        @Nullable Collection<KeyCacheObject> skipKeys
    ) {
        assert type != null || fields;
        assert clause != null || (type == SCAN || type == SET || type == INDEX);
        assert clsName != null || fields || type == SCAN || type == SET;

        this.cacheId = cacheId;
        this.id = id;
        this.cacheName = cacheName;
        this.type = type;
        this.clause = clause;
        this.idxQryDesc = idxQryDesc;
        this.limit = limit;
        this.clsName = clsName;
        this.keyValFilter = keyValFilter;
        this.part = part == null ? -1 : part;
        this.rdc = rdc;
        this.trans = trans;
        this.pageSize = pageSize;
        this.incBackups = incBackups;
        this.args = args;
        this.incMeta = incMeta;
        this.keepBinary = keepBinary;
        this.taskHash = taskHash;
        this.topVer = topVer;
        this.addDepInfo = addDepInfo;
        this.skipKeys = skipKeys;

        flags = setDataPageScanEnabled(flags, dataPageScanEnabled);
    }

    /**
     * @param flags Flags.
     * @param enabled If data page scan enabled.
     * @return Updated flags.
     */
    private static byte setDataPageScanEnabled(int flags, Boolean enabled) {
        int x = enabled == null ? FLAG_DATA_PAGE_SCAN_DFLT :
            enabled ? FLAG_DATA_PAGE_SCAN_ENABLED : FLAG_DATA_PAGE_SCAN_DISABLED;

        flags &= ~FLAG_DATA_PAGE_SCAN_MASK; // Clear old bits.
        flags |= x; // Set new bits.

        return (byte)flags;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        return topVer != null ? topVer : AffinityTopologyVersion.NONE;
    }

    /**
     * @param topVer Topology version.
     */
    public void topologyVersion(AffinityTopologyVersion topVer) {
        this.topVer = topVer;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        GridCacheContext<?, ?> cctx = ctx.cacheContext(cacheId);

        if (keyValFilter != null && keyValFilterBytes == null) {
            if (addDepInfo)
                prepareObject(keyValFilter, cctx);

            keyValFilterBytes = CU.marshal(cctx, keyValFilter);
        }

        if (rdc != null && rdcBytes == null) {
            if (addDepInfo)
                prepareObject(rdc, cctx);

            rdcBytes = CU.marshal(cctx, rdc);
        }

        if (trans != null && transBytes == null) {
            if (addDepInfo)
                prepareObject(trans, cctx);

            transBytes = CU.marshal(cctx, trans);
        }

        if (!F.isEmpty(args) && argsBytes == null) {
            if (addDepInfo) {
                for (Object arg : args)
                    prepareObject(arg, cctx);
            }

            argsBytes = CU.marshal(cctx, args);
        }

        if (idxQryDesc != null && idxQryDescBytes == null) {
            if (addDepInfo)
                prepareObject(idxQryDesc, cctx);

            idxQryDescBytes = CU.marshal(cctx, idxQryDesc);
        }

        if (!F.isEmpty(skipKeys)) {
            for (KeyCacheObject k : skipKeys)
                k.prepareMarshal(cctx.cacheObjectContext());
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        Marshaller mrsh = ctx.marshaller();

        ClassLoader clsLdr = U.resolveClassLoader(ldr, ctx.gridConfig());

        if (keyValFilterBytes != null && keyValFilter == null)
            keyValFilter = U.unmarshal(mrsh, keyValFilterBytes, clsLdr);

        if (rdcBytes != null && rdc == null)
            rdc = U.unmarshal(mrsh, rdcBytes, ldr);

        if (transBytes != null && trans == null)
            trans = U.unmarshal(mrsh, transBytes, clsLdr);

        if (argsBytes != null && args == null)
            args = U.unmarshal(mrsh, argsBytes, clsLdr);

        if (idxQryDescBytes != null && idxQryDesc == null)
            idxQryDesc = U.unmarshal(mrsh, idxQryDescBytes, clsLdr);

        if (!F.isEmpty(skipKeys)) {
            CacheObjectContext objCtx = ctx.cacheObjectContext(cacheId);

            for (KeyCacheObject k : skipKeys)
                k.finishUnmarshal(objCtx, ldr);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return addDepInfo;
    }

    /**
     * @param ctx Context.
     * @throws IgniteCheckedException In case of error.
     */
    void beforeLocalExecution(GridCacheContext<?, ?> ctx) throws IgniteCheckedException {
        Marshaller marsh = ctx.marshaller();

        rdc = rdc != null ? U.unmarshal(marsh, U.marshal(marsh, rdc),
            U.resolveClassLoader(ctx.gridConfig())) : null;
        trans = trans != null ? U.<IgniteClosure<Object, Object>>unmarshal(marsh, U.marshal(marsh, trans),
            U.resolveClassLoader(ctx.gridConfig())) : null;
        idxQryDesc = idxQryDesc != null ? U.unmarshal(marsh, U.marshal(marsh, idxQryDesc),
            U.resolveClassLoader(ctx.gridConfig())) : null;
    }

    /**
     * @return Request ID.
     */
    public long id() {
        return id;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Query type.
     */
    public GridCacheQueryType type() {
        return type;
    }

    /**
     * @return Query limit.
     */
    public int limit() {
        return limit;
    }

    /**
     * @return Query clause.
     */
    public String clause() {
        return clause;
    }

    /**
     * @return Index query description.
     */
    public IndexQueryDesc idxQryDesc() {
        return idxQryDesc;
    }

    /**
     * @return Class name.
     */
    public String className() {
        return clsName;
    }

    /**
     * @return Flag indicating whether to include backups.
     */
    public boolean includeBackups() {
        return incBackups;
    }

    /**
     * @return Flag indicating that this is cancel request.
     */
    public boolean cancel() {
        return cancel;
    }

    /**
     * @return Key-value filter.
     */
    public IgniteBiPredicate<Object, Object> keyValueFilter() {
        return keyValFilter;
    }

    /**
     * @return Reducer.
     */
    public IgniteReducer<Object, Object> reducer() {
        return rdc;
    }

    /**
     * @return Transformer.
     */
    public IgniteClosure<?, ?> transformer() {
        return trans;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @return Arguments.
     */
    public Object[] arguments() {
        return args;
    }

    /**
     * @return Include meta data or not.
     */
    public boolean includeMetaData() {
        return incMeta;
    }

    /**
     * @return Whether to load all pages.
     */
    public boolean allPages() {
        return all;
    }

    /**
     * @return Whether to keep binary.
     */
    public boolean keepBinary() {
        return keepBinary;
    }

    /**
     * @return Task hash.
     */
    public int taskHash() {
        return taskHash;
    }

    /**
     * @return Flag to enable data page scan.
     */
    public Boolean isDataPageScanEnabled() {
        switch (flags & FLAG_DATA_PAGE_SCAN_MASK) {
            case FLAG_DATA_PAGE_SCAN_ENABLED:
                return true;

            case FLAG_DATA_PAGE_SCAN_DISABLED:
                return false;
        }

        return null;
    }

    /** @return Set of keys that must be skiped during iteration. */
    public Collection<KeyCacheObject> skipKeys() {
        return skipKeys;
    }

    /**
     * @return Partition.
     */
    @Override public int partition() {
        return part;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 58;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheQueryRequest.class, this, super.toString());
    }
}
