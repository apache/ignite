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
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexSearchRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.InnerIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.LeafIO;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.ThreadLocalSchemaHolder;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.cache.query.index.sorted.SortOrder;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexSchema;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
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
    private final CacheGroupContext grp;

    /** */
    private final InlineRecommender recommender;

    /** */
    private final boolean created;

    // As it used to instantiate the tree in BackgroundTask, so use reference to get access
    // for schema and row comparator.
    /** */
    private final SortedIndexDefinition def;

    /**
     * Constructor.
     */
    public InlineIndexTree(
        SortedIndexDefinition def,
        CacheGroupContext grp,
        String treeName,
        ReuseList reuseList,
        long metaPageId,
        boolean initNew,
        int configuredInlineSize,
        InlineRecommender recommender) throws IgniteCheckedException {
        super(
            treeName,
            grp.groupId(),
            grp.name(),
            grp.dataRegion().pageMemory(),
            grp.dataRegion().config().isPersistenceEnabled() ? grp.shared().wal() : null,
            grp.offheap().globalRemoveId(),
            metaPageId,
            reuseList,
            PageIdAllocator.FLAG_IDX,
            grp.shared().kernalContext().failure(),
            null,
            PageIoResolver.DEFAULT_PAGE_IO_RESOLVER
        );
        created = initNew;

        this.def = def;

        if (!initNew) {
            // Init from metastore
            // Page is ready - read meta information.
            MetaPageInfo metaInfo = getMetaInfo();

            // TODO: if key column is complex (PK + AffinityColumn)
            //          before some moment (version=3?) index stores for inline key as object (store hash)
            //          but in one momemnt starts to unwrap it (decomplex on 2 columns and store values)
            //          This info how to compare values is set with the flag.
//        unwrappedPk = metaInfo.useUnwrappedPk();

//        cols = (unwrappedPk ? unwrappedCols : wrappedCols).toArray(H2Utils.EMPTY_COLUMNS);

            inlineSize = metaInfo.inlineSize();

            boolean inlineObjSupported = inlineSize > 0 && metaInfo.inlineObjectSupported();

            if (!metaInfo.flagsSupported())
                upgradeMetaPage(inlineObjSupported);

        } else {
            // TODO: check computeInlineSize todos
            inlineSize = computeInlineSize(
                def.getSchema().getInlineKeys(), configuredInlineSize, grp.config().getSqlIndexMaxInlineSize());

        }

        if (inlineSize == 0)
            setIos(InnerIO.VERSIONS, LeafIO.VERSIONS);
        else
            setIos(
                // -1 is required as payload starts with 1, and indexes in list of IOs are with 0.
                (IOVersions<BPlusInnerIO<IndexSearchRow>>) PageIO.getInnerVersions(inlineSize - 1, false),
                (IOVersions<BPlusLeafIO<IndexSearchRow>>) PageIO.getLeafVersions(inlineSize - 1, false));


        initTree(initNew, inlineSize);

        this.grp = grp;

        this.recommender = recommender;
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<IndexSearchRow> io, long pageAddr, int idx, IndexSearchRow row)
        throws IgniteCheckedException {

        int searchKeysLength = row.getSearchKeysCount();

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
                if (i >= schema.getInlineKeys().length) {
                    lastIdxUsed = i;
                    break;
                }

                int maxSize = inlineSize - fieldOff;

                int off = io.offset(idx);

                // TODO: Put comparator there. NULLs, ASC / DESC, ignore case

                InlineIndexKeyType keyType = schema.getInlineKeys()[i].getInlineType();

                int schemaType = keyType.type();
                int searchType = InlineIndexKeyTypeRegistry.get(row.getKey(i).getClass()).type();

                int cmp = COMPARE_UNSUPPORTED;

                // TODO: by default do not compare different types.
                if (schemaType == searchType)
                    cmp = keyType.compare(pageAddr, off + fieldOff, maxSize, row.getKey(i));

                // Can't compare as inlined bytes are not enough for comparation.
                if (cmp == CANT_BE_COMPARE) {
                    lastIdxUsed = i;
                    break;
                }

                // Try compare stored values for inlined keys with different approach?
                // TODO: looks unclear
                if (cmp == COMPARE_UNSUPPORTED)
                    cmp = def.getRowComparator().compareKey(
                        pageAddr, off + fieldOff, maxSize, row.getKey(i), keyType.type());

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
     * @param cfgInlineSize Inline size from index config. // TODO, it ignores cache.setMaxSqlInlineIndex?
     * @param maxInlineSize Max inline size from cache config. // TODO
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
            InlineIndexKeyType keyType = keyDef.getInlineType();

            if (keyType.inlineSize() <= 0) {
                size = propSize;
                break;
            }

            size += keyType.inlineSize();
        }

        return Math.min(PageIO.MAX_PAYLOAD_SIZE, size);
    }

    /** */
    public CacheGroupContext getContext() {
        return grp;
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
}
