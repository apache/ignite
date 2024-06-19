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

package org.apache.ignite.internal.cache.query.index.sorted.inline.io;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.ThreadLocalRowHandlerHolder;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

/**
 * Leaf page to store index rows with inlined keys.
 */
public abstract class AbstractInlineLeafIO extends BPlusLeafIO<IndexRow> implements InlineIO {
    /**
     * Amount of bytes to store inlined index keys.
     *
     * We do not store schema there:
     * 1. IOs are shared between multiple indexes with the same inlineSize.
     * 2. For backward compatibility, to restore index from PDS.
     */
    private final int inlineSize;

    /**
     * @param type Page type.
     * @param ver Page format version.
     * @param metaSize Size of item - information of a cache row.
     * @param inlineSize Size of calculated inlined index keys.
     */
    AbstractInlineLeafIO(short type, int ver, int metaSize, int inlineSize) {
        super(type, ver, metaSize + inlineSize);

        this.inlineSize = inlineSize;
    }

    /**
     * Register IOs for every available {@link #inlineSize}.
     */
    public static void register() {
        for (short payload = 1; payload <= PageIO.MAX_PAYLOAD_SIZE; payload++) {
            short ioType = (short)(PageIO.T_H2_EX_REF_LEAF_START + payload - 1);

            IOVersions<? extends AbstractInlineLeafIO> versions = new IOVersions<>(new InlineLeafIO(ioType, payload));

            PageIO.registerH2ExtraLeaf(versions);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override public final void storeByOffset(long pageAddr, int off, IndexRow row) {
        assert row.link() != 0 : row;
        assertPageType(pageAddr);

        int fieldOff = 0;

        InlineIndexRowHandler rowHnd = ThreadLocalRowHandlerHolder.rowHandler();

        for (int i = 0; i < rowHnd.inlineIndexKeyTypes().size(); i++) {
            try {
                InlineIndexKeyType keyType = rowHnd.inlineIndexKeyTypes().get(i);

                int size = keyType.put(pageAddr, off + fieldOff, row.key(i), inlineSize - fieldOff);

                // Inline size has exceeded.
                if (size == 0)
                    break;

                fieldOff += size;

            }
            catch (Exception e) {
                throw new IgniteException("Failed to store new index row.", e);
            }
        }

        IORowHandler.store(pageAddr, off + inlineSize, row);
    }

    /** {@inheritDoc} */
    @Override public final IndexRow getLookupRow(BPlusTree<IndexRow, ?> tree, long pageAddr, int idx)
        throws IgniteCheckedException {

        long link = PageUtils.getLong(pageAddr, offset(idx) + inlineSize);

        assert link != 0;

        return ((InlineIndexTree)tree).createIndexRow(link);
    }

    /** {@inheritDoc} */
    @Override public final void store(long dstPageAddr, int dstIdx, BPlusIO<IndexRow> srcIo, long srcPageAddr, int srcIdx) {
        assertPageType(dstPageAddr);

        int srcOff = srcIo.offset(srcIdx);

        byte[] payload = PageUtils.getBytes(srcPageAddr, srcOff, inlineSize);

        int dstOff = offset(dstIdx);

        PageUtils.putBytes(dstPageAddr, dstOff, payload);

        IORowHandler.store(dstPageAddr, dstOff + inlineSize, (InlineIO)srcIo, srcPageAddr, srcIdx);
    }

    /** {@inheritDoc} */
    @Override public long link(long pageAddr, int idx) {
        return PageUtils.getLong(pageAddr, offset(idx) + inlineSize);
    }

    /** {@inheritDoc} */
    @Override public int inlineSize() {
        return inlineSize;
    }

    /**
     * @param payload Payload size.
     * @return IOVersions for given payload.
     */
    public static IOVersions<? extends BPlusLeafIO<IndexRow>> versions(int payload) {
        assert payload >= 0 && payload <= PageIO.MAX_PAYLOAD_SIZE;

        return payload == 0 ? LeafIO.VERSIONS :
            (IOVersions<BPlusLeafIO<IndexRow>>)PageIO.getLeafVersions((short)(payload - 1));
    }
}
