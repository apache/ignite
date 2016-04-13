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

package org.apache.ignite.internal.processors.cache.database.freelist.io;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeItem;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeTree;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;

/**
 * Routines for free list leaf pages.
 */
public class FreeLeafIO extends BPlusLeafIO<FreeItem> implements FreeIO {
    /** */
    public static final IOVersions<FreeLeafIO> VERSIONS = new IOVersions<>(
        new FreeLeafIO(1)
    );

    /**
     * @param ver Page format version.
     */
    protected FreeLeafIO(int ver) {
        super(T_FREE_LEAF, ver, 12); // freeSpace(2) + dispersion(2) + pageId(8)
    }

    /** {@inheritDoc} */
    @Override public final void store(ByteBuffer buf, int idx, FreeItem row) {
        int off = offset(idx);

        buf.putInt(off, row.dispersedFreeSpace());
        buf.putLong(off + 4, row.pageId());
    }

    /** {@inheritDoc} */
    @Override public int dispersedFreeSpace(ByteBuffer buf, int idx) {
        int off = offset(idx);

        return buf.getInt(off);
    }

    /** {@inheritDoc} */
    @Override public FreeItem getLookupRow(BPlusTree<FreeItem, ?> tree, ByteBuffer buf, int idx) {
        int off = offset(idx);

        return new FreeItem(buf.getShort(off), buf.getShort(off + 2), buf.getLong(off + 4),
            ((FreeTree)tree).cacheId());
    }
}
