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

package org.apache.ignite.internal.processors.query.h2.database.io;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;
import org.apache.ignite.internal.processors.query.h2.database.H2Tree;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.h2.result.SearchRow;

/**
 * Inner page for H2 row references.
 */
public class H2InnerIO extends BPlusInnerIO<SearchRow> implements H2RowLinkIO {
    /** */
    public static final IOVersions<H2InnerIO> VERSIONS = new IOVersions<>(
        new H2InnerIO(1)
    );

    /**
     * @param ver Page format version.
     */
    private H2InnerIO(int ver) {
        super(T_H2_REF_INNER, ver, true, 8);
    }

    /** {@inheritDoc} */
    @Override public void store(ByteBuffer buf, int idx, SearchRow row) {
        GridH2Row row0 = (GridH2Row)row;

        assert row0.link != 0;

        setLink(buf, idx, row0.link);
    }

    /** {@inheritDoc} */
    @Override public SearchRow getLookupRow(BPlusTree<SearchRow,?> tree, ByteBuffer buf, int idx)
        throws IgniteCheckedException {
        long link = getLink(buf, idx);

        return ((H2Tree)tree).getRowStore().getRow(link);
    }

    /** {@inheritDoc} */
    @Override public void store(ByteBuffer dst, int dstIdx, BPlusIO<SearchRow> srcIo, ByteBuffer src, int srcIdx) {
        long link = ((H2RowLinkIO)srcIo).getLink(src, srcIdx);

        setLink(dst, dstIdx, link);
    }

    /** {@inheritDoc} */
    @Override public long getLink(ByteBuffer buf, int idx) {
        return buf.getLong(offset(idx));
    }

    /** {@inheritDoc} */
    @Override public void setLink(ByteBuffer buf, int idx, long link) {
        buf.putLong(offset(idx), link);

        assert getLink(buf, idx) == link;
    }
}
