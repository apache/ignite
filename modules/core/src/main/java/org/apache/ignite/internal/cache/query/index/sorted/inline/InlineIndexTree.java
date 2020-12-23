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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.index.sorted.SortOrder;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexSchema;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexSearchRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.LeafIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.ThreadLocalSchemaHolder;
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
public class InlineIndexTree extends BPlusTree<IndexSearchRow, IndexSearchRow> {
    /** Amount of bytes to store inlined index keys. */
    private final int inlineSize;

    /** */
    private final InlineRecommender recommender;

    /** */
    private final boolean created;

    /** */
    private final SortedIndexDefinition def;

    /** */
    private final GridCacheContext cctx;

    /** */
    private final IoStatisticsHolder stats;

    /**
     * Constructor.
     */
    public InlineIndexTree(
        SortedIndexDefinition def,
        GridCacheContext cctx,
        String treeName,
        IgniteCacheOffheapManager offheap,
        ReuseList reuseList,
        PageMemory pageMemory,
        PageIoResolver pageIoResolver,
        long metaPageId,
        boolean initNew,
        int configuredInlineSize,
        IoStatisticsHolder stats,
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

            this.def.setUseUnwrappedPk(metaInfo.useUnwrappedPk());

            inlineSize = metaInfo.inlineSize();

            boolean inlineObjSupported = inlineSize > 0 && metaInfo.inlineObjectSupported();

            if (!metaInfo.flagsSupported())
                upgradeMetaPage(inlineObjSupported);

        } else {
            this.def.setUseUnwrappedPk(true);

            inlineSize = computeInlineSize(
                def.getSchema().getKeyDefinitions(), configuredInlineSize, cctx.config().getSqlIndexMaxInlineSize());
        }

        if (inlineSize == 0)
            setIos(InnerIO.VERSIONS, LeafIO.VERSIONS);
        else
            setIos(
                // -1 is required as payload starts with 1, and indexes in list of IOs are with 0.
                (IOVersions<BPlusInnerIO<IndexSearchRow>>) PageIO.getInnerVersions(inlineSize - 1, false),
                (IOVersions<BPlusLeafIO<IndexSearchRow>>) PageIO.getLeafVersions(inlineSize - 1, false));

        initTree(initNew, inlineSize);

        this.recommender = recommender;

        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<IndexSearchRow> io, long pageAddr, int idx, IndexSearchRow row)
        throws IgniteCheckedException {

        int searchKeysLength = row.getSearchKeysCount();

        if (inlineSize == 0)
            return compareFullRows(getRow(io, pageAddr, idx), row, 0, searchKeysLength);

        SortedIndexSchema schema = def.getSchema();

        if ((schema.getKeyDefinitions().length != searchKeysLength) && row.isFullSchemaSearch())
            throw new IgniteCheckedException("Find is configured for full schema search.");

        int fieldOff = 0;

        // Use it when can't compare values (variable length, for example).
        int lastIdxUsed = searchKeysLength;

        for (int i = 0; i < searchKeysLength; i++) {
            try {
                // If a search key is null then skip other keys (consider that null shows that we should get all
                // possible keys for that comparison).
                if (row.getKey(i) == null)
                    return 0;

                // Other keys are not inlined. Should compare as rows.
                if (i >= schema.getKeyDefinitions().length) {
                    lastIdxUsed = i;
                    break;
                }

                int maxSize = inlineSize - fieldOff;

                int off = io.offset(idx);

                IndexKeyDefinition keyDef = schema.getKeyDefinitions()[i];

                if (!InlineIndexKeyTypeRegistry.supportInline(keyDef.getIdxType())) {
                    lastIdxUsed = i;
                    break;
                }

                int cmp = COMPARE_UNSUPPORTED;

                InlineIndexKeyType keyType = InlineIndexKeyTypeRegistry.get(keyDef.getIdxType());

                // By default do not compare different types.
                if (InlineIndexKeyTypeRegistry.validate(keyDef.getIdxType(), row.getKey(i).getClass()))
                    cmp = keyType.compare(pageAddr, off + fieldOff, maxSize, row.getKey(i));

                // Can't compare as inlined bytes are not enough for comparation.
                if (cmp == CANT_BE_COMPARE) {
                    lastIdxUsed = i;
                    break;
                }

                // Try compare stored values for inlined keys with different approach?
                // TODO: what to do if compare unsupported returns from row comparator?
                if (cmp == COMPARE_UNSUPPORTED)
                    cmp = def.getRowComparator().compareKey(
                        pageAddr, off + fieldOff, maxSize, row.getKey(i), keyType.type());

                if (cmp == CANT_BE_COMPARE) {
                    lastIdxUsed = i;
                    break;
                }

                if (cmp != 0)
                    return applySortOrder(cmp, schema.getKeyDefinitions()[i].getOrder().getSortOrder());

                fieldOff += keyType.inlineSize(pageAddr, off + fieldOff);

            } catch (Exception e) {
                throw new IgniteException("Failed to store new index row.", e);
            }
        }

        if (lastIdxUsed < searchKeysLength) {
            recommender.recommend(row, inlineSize);

            IndexSearchRow currRow = getRow(io, pageAddr, idx);

            for (int i = lastIdxUsed; i < searchKeysLength; i++) {
                // If a search key is null then skip other keys (consider that null shows that we should get all
                // possible keys for that comparison).
                if (row.getKey(i) == null)
                    return 0;

                int c = def.getRowComparator().compareKey(currRow, row, i);

                if (c != 0)
                    return applySortOrder(Integer.signum(c), schema.getKeyDefinitions()[i].getOrder().getSortOrder());
            }
        }

        return 0;
    }

    /** */
    private int compareFullRows(IndexSearchRow currRow, IndexSearchRow row, int from, int searchKeysLength) throws IgniteCheckedException {
        for (int i = from; i < searchKeysLength; i++) {
            // If a search key is null then skip other keys (consider that null shows that we should get all
            // possible keys for that comparison).
            if (row.getKey(i) == null)
                return 0;

            int c = def.getRowComparator().compareKey(currRow, row, i);

            if (c != 0)
                return applySortOrder(Integer.signum(c), def.getSchema().getKeyDefinitions()[i].getOrder().getSortOrder());
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
    @Override public IndexSearchRow getRow(BPlusIO<IndexSearchRow> io, long pageAddr, int idx, Object ignore)
        throws IgniteCheckedException {

        boolean cleanSchema = false;

        if (ThreadLocalSchemaHolder.getSchema() == null) {
            ThreadLocalSchemaHolder.setSchema(def.getSchema());
            cleanSchema = true;
        }

        try {
            return io.getLookupRow(this, pageAddr, idx);
        }
        finally {
            if (cleanSchema)
                ThreadLocalSchemaHolder.cleanSchema();
        }

    }

    /** */
    public int getInlineSize() {
        return inlineSize;
    }

    /**
     * @param keyDefs definition on index keys.
     * @param cfgInlineSize Inline size from index config.
     * @param maxInlineSize Max inline size from cache config.
     * @return Inline size.
     */
    public static int computeInlineSize(
        IndexKeyDefinition[] keyDefs,
        int cfgInlineSize,
        int maxInlineSize
    ) {
        if (cfgInlineSize == 0)
            return 0;

        if (F.isEmpty(keyDefs))
            return 0;

        if (cfgInlineSize != -1)
            return Math.min(PageIO.MAX_PAYLOAD_SIZE, cfgInlineSize);

        int propSize = maxInlineSize == -1
            ? IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE, IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT)
            : maxInlineSize;

        int size = 0;

        for (IndexKeyDefinition keyDef : keyDefs) {
            if (!InlineIndexKeyTypeRegistry.supportInline(keyDef.getIdxType())) {
                size = propSize;
                break;
            }

            InlineIndexKeyType keyType = InlineIndexKeyTypeRegistry.get(keyDef.getIdxType());

            if (keyType.inlineSize() <= 0) {
                size = propSize;
                break;
            }

            size += keyType.inlineSize();
        }

        return Math.min(PageIO.MAX_PAYLOAD_SIZE, size);
    }

    /** */
    public GridCacheContext getContext() {
        return cctx;
    }

    /** Default value for {@code IGNITE_MAX_INDEX_PAYLOAD_SIZE} */
    public static final int IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT = 10;

    /**
     * @return Inline size.
     * @throws IgniteCheckedException If failed.
     */
    private MetaPageInfo getMetaInfo() throws IgniteCheckedException {
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
}
