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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.index.sorted.SortOrder;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypes;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandlerFactory;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.LeafIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.ThreadLocalRowHandlerHolder;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIoResolver;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.cache.query.index.sorted.inline.keys.NullableInlineIndexKeyType.CANT_BE_COMPARE;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.keys.NullableInlineIndexKeyType.COMPARE_UNSUPPORTED;

/**
 * BPlusTree where nodes stores inlined index keys.
 */
public class InlineIndexTree extends BPlusTree<IndexRow, IndexRow> {
    /** Amount of bytes to store inlined index keys. */
    private final int inlineSize;

    /** Index key type settings for this tree. */
    private final IndexKeyTypeSettings keyTypeSettings;

    /** Recommends change inline size if needed. */
    private final InlineRecommender recommender;

    /** Whether tree is created from scratch or reused from underlying store. */
    private final boolean created;

    /** Definition of index. */
    private final SortedIndexDefinition def;

    /** */
    private final InlineIndexRowHandler rowHnd;

    /** Cache context. */
    private final GridCacheContext<?, ?> cctx;

    /** Statistics holder used by underlying BPlusTree. */
    private final IoStatisticsHolder stats;

    /**
     * Constructor.
     */
    public InlineIndexTree(
        SortedIndexDefinition def,
        GridCacheContext<?, ?> cctx,
        String treeName,
        IgniteCacheOffheapManager offheap,
        ReuseList reuseList,
        PageMemory pageMemory,
        PageIoResolver pageIoResolver,
        long metaPageId,
        boolean initNew,
        int configuredInlineSize,
        boolean strOptimizedCompare,
        IoStatisticsHolder stats,
        InlineIndexRowHandlerFactory rowHndFactory,
        InlineRecommender recommender) throws IgniteCheckedException {
        super(
            treeName,
            cctx.groupId(),
            cctx.group().name(),
            pageMemory,
            cctx.shared().wal(),
            offheap.globalRemoveId(),
            metaPageId,
            reuseList,
            PageIdAllocator.FLAG_IDX,
            cctx.shared().kernalContext().failure(),
            null,
            pageIoResolver
        );

        this.stats = stats;

        created = initNew;

        this.def = def;

        if (!initNew) {
            // Init from metastore
            // Page is ready - read meta information.
            MetaPageInfo metaInfo = getMetaInfo();

            inlineSize = metaInfo.inlineSize();

            boolean inlineObjSupported = inlineSize > 0 && metaInfo.inlineObjectSupported();

            keyTypeSettings = new IndexKeyTypeSettings(
                metaInfo.inlineObjHash, metaInfo.inlineObjSupported, strOptimizedCompare);

            rowHnd = rowHndFactory
                .create(def, metaInfo.useUnwrappedPk(), keyTypeSettings);

            if (!metaInfo.flagsSupported())
                upgradeMetaPage(inlineObjSupported);

        } else {
            keyTypeSettings = new IndexKeyTypeSettings(true, true, strOptimizedCompare);

            rowHnd = rowHndFactory.create(def, true, keyTypeSettings);

            inlineSize = computeInlineSize(
                rowHnd.getInlineIndexKeyTypes(), configuredInlineSize, cctx.config().getSqlIndexMaxInlineSize());
        }

        if (inlineSize == 0)
            setIos(InnerIO.VERSIONS, LeafIO.VERSIONS);
        else
            setIos(
                // -1 is required as payload starts with 1, and indexes in list of IOs are with 0.
                (IOVersions<BPlusInnerIO<IndexRow>>) PageIO.getInnerVersions(inlineSize - 1, false),
                (IOVersions<BPlusLeafIO<IndexRow>>) PageIO.getLeafVersions(inlineSize - 1, false));

        initTree(initNew, inlineSize);

        this.recommender = recommender;

        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<IndexRow> io, long pageAddr, int idx, IndexRow row)
        throws IgniteCheckedException {

        int searchKeysLength = row.getKeys().length;

        if (inlineSize == 0)
            return compareFullRows(getRow(io, pageAddr, idx), row, 0, searchKeysLength);

        int fieldOff = 0;

        // Use it when can't compare values (variable length, for example).
        int keyIdx;

        IndexRow currRow = null;

        int off = io.offset(idx);

        List<IndexKeyDefinition> keyDefs = def.getIndexKeyDefinitions();

        List<InlineIndexKeyType> keyTypes = rowHnd.getInlineIndexKeyTypes();

        for (keyIdx = 0; keyIdx < searchKeysLength; keyIdx++) {
            try {
                // If a search key is null then skip other keys (consider that null shows that we should get all
                // possible keys for that comparison).
                if (row.getKey(keyIdx) == null)
                    return 0;

                // Other keys are not inlined. Should compare as rows.
                if (keyIdx >= keyTypes.size())
                    break;

                int maxSize = inlineSize - fieldOff;

                InlineIndexKeyType keyType = keyTypes.get(keyIdx);

                int cmp = COMPARE_UNSUPPORTED;

                // By default do not compare different classes.
                if (!keyDefs.get(keyIdx).validate(row.getKey(keyIdx)))
                    break;

                // Value can be set up by user in query with different data type.
                // By default do not compare different types.
                if (InlineIndexKeyTypeRegistry.validate(keyType.type(), row.getKey(keyIdx).getClass())) {
                    if (keyType.type() != IndexKeyTypes.JAVA_OBJECT || keyTypeSettings.inlineObjSupported()) {
                        cmp = keyType.compare(pageAddr, off + fieldOff, maxSize, row.getKey(keyIdx));

                        fieldOff += keyType.inlineSize(pageAddr, off + fieldOff);
                    }
                    // If inlining of POJO is not supported then fallback to previous logic.
                    else {
                        if (currRow == null)
                            currRow = getRow(io, pageAddr, idx);

                        cmp = compareFullRows(currRow, row, keyIdx, keyIdx + 1);
                    }
                }

                // Can't compare as inlined bytes are not enough for comparation.
                if (cmp == CANT_BE_COMPARE)
                    break;

                // Try compare stored values for inlined keys with different approach?
                if (cmp == COMPARE_UNSUPPORTED)
                    cmp = def.getRowComparator().compareKey(
                        pageAddr, off + fieldOff, maxSize, row.getKey(keyIdx), keyType.type());

                if (cmp == CANT_BE_COMPARE || cmp == COMPARE_UNSUPPORTED)
                    break;

                if (cmp != 0)
                    return applySortOrder(cmp, keyDefs.get(keyIdx).getOrder().getSortOrder());

            } catch (Exception e) {
                throw new IgniteException("Failed to store new index row.", e);
            }
        }

        if (keyIdx < searchKeysLength) {
            recommender.recommend(row, inlineSize);

            if (currRow == null)
                currRow = getRow(io, pageAddr, idx);

            return compareFullRows(currRow, row, keyIdx, searchKeysLength);
        }

        return 0;
    }

    /** */
    private int compareFullRows(IndexRow currRow, IndexRow row, int from, int searchKeysLength) throws IgniteCheckedException {
        for (int i = from; i < searchKeysLength; i++) {
            // If a search key is null then skip other keys (consider that null shows that we should get all
            // possible keys for that comparison).
            if (row.getKey(i) == null)
                return 0;

            int c = def.getRowComparator().compareKey(currRow, row, i);

            if (c != 0)
                return applySortOrder(Integer.signum(c), def.getIndexKeyDefinitions().get(i).getOrder().getSortOrder());
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

    /** {@inheritDoc} */
    @Override public IndexRow getRow(BPlusIO<IndexRow> io, long pageAddr, int idx, Object ignore)
        throws IgniteCheckedException {

        boolean cleanSchema = false;

        if (ThreadLocalRowHandlerHolder.getRowHandler() == null) {
            ThreadLocalRowHandlerHolder.setRowHandler(rowHnd);
            cleanSchema = true;
        }

        try {
            return io.getLookupRow(this, pageAddr, idx);
        }
        finally {
            if (cleanSchema)
                ThreadLocalRowHandlerHolder.clearRowHandler();
        }
    }

    /** */
    public int getInlineSize() {
        return inlineSize;
    }

    /**
     * @param keyTypes Index key types.
     * @param cfgInlineSize Inline size from index config.
     * @param maxInlineSize Max inline size from cache config.
     * @return Inline size.
     */
    public static int computeInlineSize(
        List<InlineIndexKeyType> keyTypes,
        int cfgInlineSize,
        int maxInlineSize
    ) {
        if (cfgInlineSize == 0)
            return 0;

        if (F.isEmpty(keyTypes))
            return 0;

        if (cfgInlineSize != -1)
            return Math.min(PageIO.MAX_PAYLOAD_SIZE, cfgInlineSize);

        int propSize = maxInlineSize == -1
            ? IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE, IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT)
            : maxInlineSize;

        int size = 0;

        for (InlineIndexKeyType keyType: keyTypes) {
            if (keyType.inlineSize() <= 0) {
                size = propSize;
                break;
            }

            size += keyType.inlineSize();
        }

        return Math.min(PageIO.MAX_PAYLOAD_SIZE, size);
    }

    /** */
    public GridCacheContext<?, ?> getContext() {
        return cctx;
    }

    /** Default value for {@code IGNITE_MAX_INDEX_PAYLOAD_SIZE} */
    public static final int IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT = 10;

    /**
     * @return Inline size.
     * @throws IgniteCheckedException If failed.
     */
    public MetaPageInfo getMetaInfo() throws IgniteCheckedException {
        final long metaPage = acquirePage(metaPageId);

        try {
            long pageAddr = readLock(metaPageId, metaPage); // Meta can't be removed.

            assert pageAddr != 0 : "Failed to read lock meta page [metaPageId=" +
                U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(pageAddr);

                return new MetaPageInfo(io, pageAddr);
            }
            finally {
                readUnlock(metaPageId, metaPage, pageAddr);
            }
        }
        finally {
            releasePage(metaPageId, metaPage);
        }
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

            assert pageAddr != 0 : "Failed to read lock meta page [metaPageId=" +
                U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO.upgradePageVersion(pageAddr, inlineObjSupported, false, pageSize());

                if (wal != null)
                    wal.log(new PageSnapshot(new FullPageId(metaPageId, grpId),
                        pageAddr, pageMem.pageSize(), pageMem.realPageSize(grpId)));
            }
            finally {
                writeUnlock(metaPageId, metaPage, pageAddr, true);
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
        final long metaPage = acquirePage(metaPageId);

        try {
            long pageAddr = writeLock(metaPageId, metaPage); // Meta can't be removed.

            assert pageAddr != 0 : "Failed to read lock meta page [metaPageId=" +
                U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO.setValues(
                    pageAddr,
                    info.inlineSize,
                    info.useUnwrappedPk,
                    info.inlineObjSupported,
                    info.inlineObjHash
                );
            }
            finally {
                writeUnlock(metaPageId, metaPage, pageAddr, true);
            }
        }
        finally {
            releasePage(metaPageId, metaPage);
        }
    }

    /** */
    public boolean isCreated() {
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
        CorruptedTreeException e = new CorruptedTreeException(msg, cause, grpId, grpName, def.getIdxName().cacheName(),
            def.getIdxName().idxName(), pageIds);

        processFailure(FailureType.CRITICAL_ERROR, e);

        return e;
    }

    /** {@inheritDoc} */
    @Override protected IoStatisticsHolder statisticsHolder() {
        return stats != null ? stats : super.statisticsHolder();
    }

    /**
     * @return Index row handler for this tree.
     */
    public InlineIndexRowHandler getRowHandler() {
        return rowHnd;
    }
}
