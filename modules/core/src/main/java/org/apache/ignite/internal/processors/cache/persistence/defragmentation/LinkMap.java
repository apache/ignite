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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;

/**
 * Class that holds mappings of old links to new links.
 */
public class LinkMap {
    /** Tree meta page index. */
    public static final int META_PAGE_IDX = 2;

    /** */
    public static final IOVersions<? extends BPlusLeafIO<?>> LEAF_IO_VERSIONS = new IOVersions<>(
        new LinkMappingLeafIO()
    );

    /** */
    public static final IOVersions<? extends BPlusInnerIO<?>> INNER_IO_VERSIONS = new IOVersions<>(
        new LinkMappingInnerIO()
    );

    /** Mapping tree. */
    private final LinkTree tree;

    /**
     * @param ctx Cache group context.
     * @param pageMem Page memory.
     * @param metaPageId Meta page id.
     * @param initNew If tree should be (re)created.
     */
    public LinkMap(
        CacheGroupContext ctx,
        PageMemory pageMem,
        long metaPageId,
        boolean initNew
    ) throws IgniteCheckedException {
        this(ctx.groupId(), ctx.name(), pageMem, metaPageId, initNew);
    }

    /**
     * @param grpId Cache group id.
     * @param grpName Cache group name.
     * @param pageMem Page memory.
     * @param metaPageId Meta page id.
     * @param initNew If tree should be (re)created.
     */
    public LinkMap(
        int grpId,
        String grpName,
        PageMemory pageMem,
        long metaPageId,
        boolean initNew
    ) throws IgniteCheckedException {
        tree = new LinkTree(
            "link-map",
            grpId,
            grpName,
            pageMem,
            null,
            new AtomicLong(),
            metaPageId,
            null,
            (IOVersions<LinkMappingInnerIO>)INNER_IO_VERSIONS,
            (IOVersions<LinkMappingLeafIO>)LEAF_IO_VERSIONS,
            null,
            null,
            initNew
        );
    }

    /**
     * Add link mapping.
     *
     * @param oldLink Old link.
     * @param newLink New link.
     */
    public void put(long oldLink, long newLink) throws IgniteCheckedException {
        tree.put(new LinkMapping(oldLink, newLink));
    }

    /**
     * Get new link by old link.
     *
     * @param oldLink Old link.
     */
    public long get(long oldLink) throws IgniteCheckedException {
        LinkMapping get = new LinkMapping(oldLink, 0);
        LinkMapping found = tree.findOne(get);

        return found.getNewLink();
    }

    /** */
    private static class LinkTree extends BPlusTree<LinkMapping, LinkMapping> {
        /**
         * @param name Tree name.
         * @param cacheGrpId Cache group ID.
         * @param cacheGrpName Cache group name.
         * @param pageMem Page memory.
         * @param wal Write ahead log manager.
         * @param globalRmvId Remove ID.
         * @param metaPageId Meta page ID.
         * @param reuseList Reuse list.
         * @param innerIos Inner IO versions.
         * @param leafIos Leaf IO versions.
         * @param failureProcessor if the tree is corrupted.
         * @param initNew If tree should be (re)created.
         *
         * @throws IgniteCheckedException If failed.
         */
        protected LinkTree(
           String name,
           int cacheGrpId,
           String cacheGrpName,
           PageMemory pageMem,
           IgniteWriteAheadLogManager wal,
           AtomicLong globalRmvId,
           long metaPageId,
           ReuseList reuseList,
           IOVersions<? extends BPlusInnerIO<LinkMapping>> innerIos,
           IOVersions<? extends BPlusLeafIO<LinkMapping>> leafIos,
           @Nullable FailureProcessor failureProcessor,
           @Nullable PageLockListener lockLsnr,
           boolean initNew
        ) throws IgniteCheckedException {
            super(
                name,
                cacheGrpId,
                cacheGrpName,
                pageMem,
                wal,
                globalRmvId,
                metaPageId,
                reuseList,
                innerIos,
                leafIos,
                FLAG_AUX,
                failureProcessor,
                lockLsnr
            );

            PageIO.registerTest(latestInnerIO(), latestLeafIO());

            initTree(initNew);
        }

        /** {@inheritDoc} */
        @Override protected int compare(BPlusIO<LinkMapping> io, long pageAddr, int idx, LinkMapping row) throws IgniteCheckedException {
            LinkMapping lookupRow = io.getLookupRow(this, pageAddr, idx);

            return Long.compare(lookupRow.getOldLink(), row.getOldLink());
        }

        /** {@inheritDoc} */
        @Override public LinkMapping getRow(BPlusIO<LinkMapping> io, long pageAddr, int idx, Object x) throws IgniteCheckedException {
            return io.getLookupRow(this, pageAddr, idx);
        }

        /** {@inheritDoc} */
        @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
            return pageMem.allocatePage(grpId, PageIdUtils.partId(metaPageId), FLAG_DATA);
        }
    }

    /**
     * Class holding mapping from old link to new link.
     */
    private static class LinkMapping {
        /** Old link. */
        private final long oldLink;

        /** New link. */
        private final long newLink;

        /**
         * @param oldLink Old link.
         * @param newLink New link.
         */
        public LinkMapping(long oldLink, long newLink) {
            this.oldLink = oldLink;
            this.newLink = newLink;
        }

        /** */
        public long getOldLink() {
            return oldLink;
        }

        /** */
        public long getNewLink() {
            return newLink;
        }
    }

    /** */
    private static class LinkMappingInnerIO extends BPlusInnerIO<LinkMapping> {
        /** */
        protected LinkMappingInnerIO() {
            super(PageIO.T_DEFRAG_LINK_MAPPING_INNER, 1, true, Long.BYTES * 2);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, LinkMapping row) {
            PageUtils.putLong(pageAddr, off, row.getOldLink());
            PageUtils.putLong(pageAddr, off + Long.BYTES, row.getNewLink());
        }

        /** {@inheritDoc} */
        @Override public void store(long dst, int dstIdx, BPlusIO<LinkMapping> srcIo, long src, int srcIdx)
                throws IgniteCheckedException {
            assert srcIo == this;

            storeByOffset(dst, offset(dstIdx), srcIo.getLookupRow(null, src, srcIdx));
        }

        /** {@inheritDoc} */
        @Override public LinkMapping getLookupRow(BPlusTree<LinkMapping, ?> tree, long pageAddr, int idx) {
            long oldLink = PageUtils.getLong(pageAddr, offset(idx));
            long newLink = PageUtils.getLong(pageAddr, offset(idx) + Long.BYTES);

            return new LinkMapping(oldLink, newLink);
        }
    }

    /** */
    private static class LinkMappingLeafIO extends BPlusLeafIO<LinkMapping> {
        /** */
        protected LinkMappingLeafIO() {
            super(PageIO.T_DEFRAG_LINK_MAPPING_LEAF, 1, Long.BYTES * 2);
        }

        /** {@inheritDoc} */
        @Override public void storeByOffset(long pageAddr, int off, LinkMapping row) {
            PageUtils.putLong(pageAddr, off, row.getOldLink());
            PageUtils.putLong(pageAddr, off + Long.BYTES, row.getNewLink());
        }

        /** {@inheritDoc} */
        @Override public void store(long dst, int dstIdx, BPlusIO<LinkMapping> srcIo, long src, int srcIdx)
                throws IgniteCheckedException {
            assert srcIo == this;

            storeByOffset(dst, offset(dstIdx), srcIo.getLookupRow(null, src, srcIdx));
        }

        /** {@inheritDoc} */
        @Override public LinkMapping getLookupRow(BPlusTree<LinkMapping, ?> tree, long pageAddr, int idx) {
            long oldLink = PageUtils.getLong(pageAddr, offset(idx));
            long newLink = PageUtils.getLong(pageAddr, offset(idx) + Long.BYTES);

            return new LinkMapping(oldLink, newLink);
        }
    }
}
