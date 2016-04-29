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

package org.apache.ignite.internal.processors.cache.database.tree.reuse.io;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.IOVersions;

/**
 * Reuse list leaf page IO routines.
 */
public final class ReuseLeafIO extends BPlusLeafIO<Number> {
    /** */
    public static final IOVersions<ReuseLeafIO> VERSIONS = new IOVersions<>(
        new ReuseLeafIO(1)
    );

    /**
     * @param ver Page format version.
     */
    protected ReuseLeafIO(int ver) {
        super(T_REUSE_LEAF, ver, 8); // TODO we can store only 6 bytes here: pageIndex(4) + partId(2)
    }

    /** {@inheritDoc} */
    @Override public void store(ByteBuffer buf, int idx, Number pageId) {
        assert pageId.getClass() == Long.class;

        buf.putLong(offset(idx), pageId.longValue());
    }

    /** {@inheritDoc} */
    @Override public void store(ByteBuffer dst, int dstIdx, BPlusIO<Number> srcIo, ByteBuffer src, int srcIdx) {
        assert srcIo == this;

        dst.putLong(offset(dstIdx), getPageId(src, srcIdx));
    }

    /**
     * @param buf Buffer.
     * @param idx Index.
     * @return Page ID.
     */
    public long getPageId(ByteBuffer buf, int idx) {
        return buf.getLong(offset(idx));
    }

    /** {@inheritDoc} */
    @Override public Number getLookupRow(BPlusTree<Number,?> tree, ByteBuffer buf, int idx)
        throws IgniteCheckedException {
        return getPageId(buf, idx);
    }
}
