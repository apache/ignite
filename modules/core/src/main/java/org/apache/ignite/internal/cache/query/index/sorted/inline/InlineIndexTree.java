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

package org.apache.ignite.internal.cache.query.index.sorted.inline;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.SortOrder;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowCache;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowComparator;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandlerFactory;
import org.apache.ignite.internal.cache.query.index.sorted.MetaPageInfo;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.ThreadLocalRowHandlerHolder;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineInnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineLeafIO;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIoResolver;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandlerWrapper;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BPLUS_TREE_DISABLE_METRICS;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl.INDEX_METRIC_PREFIX;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.NullableInlineIndexKeyType.CANT_BE_COMPARE;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.NullableInlineIndexKeyType.COMPARE_UNSUPPORTED;
import static org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexUtils.mergeTasks;
import static org.apache.ignite.internal.cache.query.index.sorted.maintenance.MaintenanceRebuildIndexUtils.toMaintenanceTask;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * BPlusTree where nodes stores inlined index keys.
 */
public class InlineIndexTree extends BPlusTree<IndexRow, IndexRow> {
    /**
     * Default sql index size for types with variable length (such as String or byte[]).
     * Note that effective length will be lower, because 3 bytes will be taken for the inner representation of variable type.
     */
    public static final int IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE = 10;

    /** Amount of bytes to store inlined index keys. */
    private final int inlineSize;

    /** Recommends change inline size if needed. */
    private final InlineRecommender recommender;

    /** Whether tree is created from scratch or reused from underlying store. */
    private final boolean created;

    /** Definition of index. */
    private final SortedIndexDefinition def;

    /** */
    private final InlineIndexRowHandler rowHnd;

    /** Cache group context. */
    private final CacheGroupContext grpCtx;

    /** Statistics holder used by underlying BPlusTree. */
    @Nullable private final IoStatisticsHolder stats;

    /** */
    private final IgniteLogger log;

    /** Row cache. */
    private final @Nullable IndexRowCache idxRowCache;

    /**
     * Constructor.
     */
    public InlineIndexTree(
        SortedIndexDefinition def,
        CacheGroupContext grpCtx,
        String treeName,
        IgniteCacheOffheapManager offheap,
        ReuseList reuseList,
        PageMemory pageMemory,
        PageIoResolver pageIoResolver,
        long metaPageId,
        boolean initNew,
        int configuredInlineSize,
        int maxInlineSize,
        IndexKeyTypeSettings keyTypeSettings,
        @Nullable IndexRowCache idxRowCache,
        @Nullable IoStatisticsHolder stats,
        InlineIndexRowHandlerFactory rowHndFactory,
        InlineRecommender recommender
    ) throws IgniteCheckedException {
        super(
            treeName,
            grpCtx.groupId(),
            grpCtx.name(),
            pageMemory,
            grpCtx.shared().wal(),
            offheap.globalRemoveId(),
            metaPageId,
            reuseList,
            PageIdAllocator.FLAG_IDX,
            grpCtx.shared().kernalContext().failure(),
            grpCtx.shared().diagnostic().pageLockTracker(),
            pageIoResolver,
            wrapper(def)
        );

        this.grpCtx = grpCtx;

        log = grpCtx.shared().kernalContext().config().getGridLogger();

        this.stats = stats;

        created = initNew;

        this.def = def;

        this.idxRowCache = idxRowCache;

        if (!initNew) {
            // Init from metastore.
            // Page is ready - read meta information.
            MetaPageInfo metaInfo = metaInfo();

            inlineSize = metaInfo.inlineSize();
            setIos(inlineSize);

            boolean inlineObjSupported = inlineObjectSupported(def, metaInfo, rowHndFactory);

            keyTypeSettings
                .inlineObjHash(metaInfo.inlineObjectHash())
                .inlineObjSupported(inlineObjSupported);

            rowHnd = rowHndFactory.create(def, keyTypeSettings);

            if (!metaInfo.flagsSupported())
                upgradeMetaPage(inlineObjSupported);
        }
        else {
            rowHnd = rowHndFactory.create(def, keyTypeSettings);

            inlineSize = computeInlineSize(
                def.idxName().fullName(),
                rowHnd.inlineIndexKeyTypes(),
                rowHnd.indexKeyDefinitions(),
                configuredInlineSize,
                maxInlineSize,
                log
            );

            setIos(inlineSize);
        }

        initTree(initNew, inlineSize);

        this.recommender = recommender;
    }

    /** */
    private void setIos(int inlineSize) {
        setIos(
            AbstractInlineInnerIO.versions(inlineSize),
            AbstractInlineLeafIO.versions(inlineSize)
        );
    }

    /**
     * Find whether tree supports inlining objects or not.
     *
     * @param def Index definition.
     * @param metaInfo Metapage info.
     * @return {@code true} if inline object is supported by exists tree.
     */
    private boolean inlineObjectSupported(SortedIndexDefinition def, MetaPageInfo metaInfo,
        InlineIndexRowHandlerFactory rowHndFactory) {

        if (metaInfo.flagsSupported())
            return metaInfo.inlineObjectSupported();
        else {
            try {
                if (InlineObjectBytesDetector.objectMayBeInlined(metaInfo.inlineSize(), def.indexKeyDefinitions().values())) {
                    try {
                        InlineObjectBytesDetector inlineObjDetector = new InlineObjectBytesDetector(
                            metaInfo.inlineSize(), def.indexKeyDefinitions().values(), def.idxName(), log);

                        // Create a settings for case where java objects inilned as byte array.
                        IndexKeyTypeSettings keyTypeSettings = new IndexKeyTypeSettings()
                            .inlineObjSupported(true)
                            .inlineObjHash(false);

                        InlineIndexRowHandler rowHnd = rowHndFactory.create(def, keyTypeSettings);

                        ThreadLocalRowHandlerHolder.rowHandler(rowHnd);

                        findFirst(inlineObjDetector);

                        return inlineObjDetector.inlineObjectSupported();

                    }
                    finally {
                        ThreadLocalRowHandlerHolder.clearRowHandler();
                    }
                }
                else
                    return false;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Unexpected exception on detect inline object", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<IndexRow> io, long pageAddr, int idx, IndexRow row)
        throws IgniteCheckedException {

        if (inlineSize == 0) {
            IndexRow currRow = getRow(io, pageAddr, idx);

            return compareFullRows(currRow, row, 0, rowHandler(), def.rowComparator());
        }

        int fieldOff = 0;

        // Use it when can't compare values (variable length, for example).
        int keyIdx;

        IndexRow currRow = null;

        int off = io.offset(idx);

        List<IndexKeyDefinition> keyDefs = rowHnd.indexKeyDefinitions();
        List<InlineIndexKeyType> keyTypes = rowHnd.inlineIndexKeyTypes();

        for (keyIdx = 0; keyIdx < keyTypes.size(); keyIdx++) {
            try {
                // If a search key is null then skip other keys (consider that null shows that we should get all
                // possible keys for that comparison).
                if (row.key(keyIdx) == null)
                    return 0;

                int maxSize = inlineSize - fieldOff;

                InlineIndexKeyType keyType = keyTypes.get(keyIdx);

                int cmp = def.rowComparator().compareKey(pageAddr, off + fieldOff, maxSize, row.key(keyIdx), keyType);

                if (cmp == CANT_BE_COMPARE || cmp == COMPARE_UNSUPPORTED)
                    break;
                else
                    fieldOff += keyType.inlineSize(pageAddr, off + fieldOff);

                if (cmp != 0) {
                    IndexKeyDefinition keyDef = keyDefs.get(keyIdx);

                    return applySortOrder(cmp, keyDef.order().sortOrder());
                }
            }
            catch (Exception e) {
                throw new IgniteException("Failed to store new index row.", e);
            }
        }

        if (keyIdx < keyDefs.size()) {
            recommender.recommend(row, inlineSize);

            if (currRow == null)
                currRow = getRow(io, pageAddr, idx);

            return compareFullRows(currRow, row, keyIdx, rowHandler(), def.rowComparator());
        }

        return 0;
    }

    /** */
    public static int compareFullRows(
        IndexRow currRow,
        IndexRow row,
        int from,
        InlineIndexRowHandler rowHnd,
        IndexRowComparator rowCmp
    ) throws IgniteCheckedException {
        if (currRow == row)
            return 0;

        List<IndexKeyDefinition> idxKeyDefs = rowHnd.indexKeyDefinitions();

        for (int i = from; i < idxKeyDefs.size(); i++) {
            // If a search key is null then skip other keys (consider that null shows that we should get all
            // possible keys for that comparison).
            if (row.key(i) == null)
                return 0;

            int c = rowCmp.compareRow(currRow, row, i);

            if (c != 0)
                return applySortOrder(Integer.signum(c), idxKeyDefs.get(i).order().sortOrder());
        }

        return 0;
    }

    /**
     * Perform sort order correction.
     *
     * @param c Compare result.
     * @param order Sort order.
     * @return Fixed compare result.
     */
    private static int applySortOrder(int c, SortOrder order) {
        return order == SortOrder.ASC ? c : -c;
    }

    /** Creates an index row for this tree. */
    public IndexRowImpl createIndexRow(long link) throws IgniteCheckedException {
        IndexRowImpl cachedRow = idxRowCache == null ? null : idxRowCache.get(link);

        if (cachedRow != null) {
            return cachedRow.rowHandler() == rowHandler() ? cachedRow :
                new IndexRowImpl(rowHandler(), cachedRow.cacheDataRow());
        }

        CacheDataRowAdapter row = new CacheDataRowAdapter(link);

        row.initFromLink(cacheGroupContext(), CacheDataRowAdapter.RowData.FULL, true);

        IndexRowImpl r = new IndexRowImpl(rowHandler(), row);

        if (idxRowCache != null)
            idxRowCache.put(r);

        return r;
    }

    /** {@inheritDoc} */
    @Override public IndexRow getRow(BPlusIO<IndexRow> io, long pageAddr, int idx, Object ignore)
        throws IgniteCheckedException {

        return io.getLookupRow(this, pageAddr, idx);
    }

    /** */
    public int inlineSize() {
        return inlineSize;
    }

    /**
     * @param name Index name.
     * @param keyTypes Index key types.
     * @param keyDefs Index key definitions.
     * @param cfgInlineSize Inline size from index config.
     * @param maxInlineSize Max inline size from cache config.
     * @param log Logger.
     * @return Inline size.
     */
    public static int computeInlineSize(
        String name,
        List<InlineIndexKeyType> keyTypes,
        List<IndexKeyDefinition> keyDefs,
        int cfgInlineSize,
        int maxInlineSize,
        IgniteLogger log
    ) {
        if (cfgInlineSize == 0)
            return 0;

        if (F.isEmpty(keyTypes))
            return 0;

        boolean fixedSize = true;

        int propSize = maxInlineSize == -1
            ? IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE, IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT)
            : maxInlineSize;

        int size = 0;

        for (int i = 0; i < keyTypes.size(); i++) {
            InlineIndexKeyType keyType = keyTypes.get(i);

            fixedSize &= keyType.keySize() != -1;

            int sizeInc = keyType.inlineSize();

            if (sizeInc < 0) {
                int precision = keyDefs.get(i).precision();

                if (precision > 0)
                    // 3 is required to store (type, length) of value.
                    sizeInc = 3 + precision;
                else
                    sizeInc = IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE;
            }

            size += sizeInc;

            if (size > propSize) {
                size = propSize;
                break;
            }
        }

        if (cfgInlineSize != -1) {
            cfgInlineSize = Math.min(PageIO.MAX_PAYLOAD_SIZE, cfgInlineSize);

            if (fixedSize && size < cfgInlineSize) {
                log.warning("Explicit INLINE_SIZE for fixed size index item is too big. " +
                    "This will lead to wasting of space inside index pages. Ignoring " +
                    "[index=" + name + ", explicitInlineSize=" + cfgInlineSize + ", realInlineSize=" + size + ']');

                return size;
            }

            return cfgInlineSize;
        }

        return Math.min(PageIO.MAX_PAYLOAD_SIZE, size);
    }

    /**
     * Getting cache group context.
     *
     * @return Cache group context.
     */
    public CacheGroupContext cacheGroupContext() {
        return grpCtx;
    }

    /** Default value for {@code IGNITE_MAX_INDEX_PAYLOAD_SIZE} */
    public static final int IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT = 64;

    /**
     * @return Inline size.
     * @throws IgniteCheckedException If failed.
     */
    public MetaPageInfo metaInfo() throws IgniteCheckedException {
        return MetaPageInfo.read(metaPageId, grpId, pageMem);
    }

    /**
     * Update root meta page if need (previous version not supported features flags
     * and created product version on root meta page).
     *
     * @param inlineObjSupported inline POJO by created tree flag.
     * @throws IgniteCheckedException On error.
     */
    private void upgradeMetaPage(boolean inlineObjSupported) throws IgniteCheckedException {
        final long metaPage = acquirePage(metaPageId);

        try {
            long pageAddr = writeLock(metaPageId, metaPage); // Meta can't be removed.

            assert pageAddr != 0 : "Failed to write lock meta page [metaPageId=" + U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO.upgradePageVersion(pageAddr, inlineObjSupported, false, pageSize());
            }
            finally {
                writeUnlock(metaPageId, metaPage, pageAddr, Boolean.TRUE, true);
            }
        }
        finally {
            releasePage(metaPageId, metaPage);
        }
    }

    /**
     * Copy info from another meta page.
     * @param info Meta page info.
     * @throws IgniteCheckedException If failed.
     */
    public void copyMetaInfo(MetaPageInfo info) throws IgniteCheckedException {
        info.write(metaPageId, grpId, pageMem);
    }

    /** */
    public boolean created() {
        return created;
    }

    /**
     * Construct the exception and invoke failure processor.
     *
     * @param msg Message.
     * @param cause Cause.
     * @param grpId Group id.
     * @param pageIds Pages ids.
     * @return New CorruptedTreeException instance.
     */
    @Override protected CorruptedTreeException corruptedTreeException(String msg, Throwable cause, int grpId, long... pageIds) {
        IndexName idx = def.idxName();

        String idxName = idx.idxName();
        String cacheName = idx.cacheName();
        String tableName = idx.tableName();

        CorruptedTreeException e = new CorruptedTreeException(msg, cause, grpName, cacheName,
            idxName, grpId, pageIds);

        if (grpCtx.persistenceEnabled()) {
            String errorMsg = "Index " + idx + " of the table " + tableName + " (cache " + cacheName + ") is " +
                    "corrupted, to fix this issue a rebuild is required. On the next restart, node will enter the " +
                    "maintenance mode and rebuild corrupted indexes.";

            log.warning(errorMsg);

            int cacheId = CU.cacheId(cacheName);

            try {
                MaintenanceTask task = toMaintenanceTask(cacheId, idxName);

                grpCtx.shared().kernalContext().maintenanceRegistry().registerMaintenanceTask(
                        task,
                        oldTask -> mergeTasks(oldTask, task)
                );
            }
            catch (IgniteCheckedException ex) {
                log.warning("Failed to register maintenance record for corrupted partition files.", ex);
            }
        }

        processFailure(FailureType.CRITICAL_ERROR, e);

        return e;
    }

    /** {@inheritDoc} */
    @Override protected void temporaryReleaseLock() {
        grpCtx.shared().database().checkpointReadUnlock();
        grpCtx.shared().database().checkpointReadLock();
    }

    /** {@inheritDoc} */
    @Override protected long maxLockHoldTime() {
        long sysWorkerBlockedTimeout = grpCtx.shared().kernalContext().workersRegistry().getSystemWorkerBlockedTimeout();

        // Using timeout value reduced by 10 times to increase possibility of lock releasing before timeout.
        return sysWorkerBlockedTimeout == 0 ? Long.MAX_VALUE : (sysWorkerBlockedTimeout / 10);
    }

    /** {@inheritDoc} */
    @Override protected IoStatisticsHolder statisticsHolder() {
        return stats != null ? stats : super.statisticsHolder();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(InlineIndexTree.class, this, "super", super.toString());
    }

    /**
     * @return Index row handler for this tree. Row handler for a tree can be set externally with the holder.
     */
    public InlineIndexRowHandler rowHandler() {
        return rowHnd != null ? rowHnd : ThreadLocalRowHandlerHolder.rowHandler();
    }

    /** {@inheritDoc} */
    @Override protected String lockRetryErrorMessage(String op) {
        IndexName idxName = def.idxName();

        return super.lockRetryErrorMessage(op) + " Problem with the index [cacheName=" + idxName.cacheName() +
            ", schemaName=" + idxName.schemaName() + ", tblName=" + idxName.tableName() + ", idxName=" +
            idxName.idxName() + ']';
    }

    /** */
    private static PageHandlerWrapper<Result> wrapper(SortedIndexDefinition def) {
        if (def == null || def.cacheInfo().cacheContext() == null)
            return null;

        if (IgniteSystemProperties.getBoolean(IGNITE_BPLUS_TREE_DISABLE_METRICS))
            return null;

        return new PageHandlerWrapper<Result>() {
            @Override public PageHandler<?, Result> wrap(BPlusTree<?, ?> tree, PageHandler<?, Result> hnd) {
                GridCacheContext<?, ?> cctx = def.cacheInfo().cacheContext();

                MetricRegistryImpl mreg = cctx.shared().kernalContext().metric().registry(
                    metricName(INDEX_METRIC_PREFIX, def.idxName().fullName()));

                LongAdderMetric cnt = mreg.longAdderMetric(hnd.getClass().getSimpleName() + "Count",
                    "Count of " + hnd.getClass().getSimpleName() + " operations");
                LongAdderMetric time = mreg.longAdderMetric(hnd.getClass().getSimpleName() + "Time",
                    "Total time of " + hnd.getClass().getSimpleName() + " operations (nanoseconds)");

                return new PageHandler<Object, Result>() {
                    @Override public Result run(
                        int cacheId,
                        long pageId,
                        long page,
                        long pageAddr,
                        PageIO io,
                        Boolean walPlc,
                        Object arg,
                        int intArg,
                        IoStatisticsHolder statHolder
                    ) throws IgniteCheckedException {
                        if (!cctx.statisticsEnabled()) {
                            return ((PageHandler<Object, Result>)hnd).run(cacheId, pageId, page, pageAddr, io, walPlc,
                                arg, intArg, statHolder);
                        }

                        long ts = System.nanoTime();

                        try {
                            return ((PageHandler<Object, Result>)hnd).run(cacheId, pageId, page, pageAddr, io, walPlc,
                                arg, intArg, statHolder);
                        }
                        finally {
                            cnt.increment();
                            time.add(System.nanoTime() - ts);
                        }
                    }

                    @Override public boolean releaseAfterWrite(
                        int cacheId,
                        long pageId,
                        long page,
                        long pageAddr,
                        Object arg,
                        int intArg
                    ) {
                        return ((PageHandler<Object, Result>)hnd).releaseAfterWrite(cacheId, pageId, page, pageAddr,
                            arg, intArg);
                    }
                };
            }
        };
    }
}
