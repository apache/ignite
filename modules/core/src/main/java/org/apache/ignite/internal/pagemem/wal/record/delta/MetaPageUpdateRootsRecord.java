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

package org.apache.ignite.internal.pagemem.wal.record.delta;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageMetaIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PagePartitionMetaIO;

/**
 *
 */
public class MetaPageUpdateRootsRecord extends PageDeltaRecord {
    /** */
    private long treeRoot;

    /** */
    private long reuseListRoot;

    /** */
    private int ioType;

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param treeRoot Tree root.
     * @param reuseListRoot Reuse list root.
     * @param ioType IO type.
     */
    public MetaPageUpdateRootsRecord(int cacheId, long pageId, long treeRoot, long reuseListRoot, int ioType) {
        super(cacheId, pageId);

        assert ioType == PageIO.T_META || ioType == PageIO.T_PART_META;

        this.treeRoot = treeRoot;
        this.reuseListRoot = reuseListRoot;
        this.ioType = ioType;
    }

    /**
     * @return Tree root.
     */
    public long treeRoot() {
        return treeRoot;
    }

    /**
     * @return Reuse list root.
     */
    public long reuseListRoot() {
        return reuseListRoot;
    }

    /**
     * @return IO type.
     */
    public int ioType() {
        return ioType;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(ByteBuffer buf) throws IgniteCheckedException {
        PageMetaIO io = ioType == PageIO.T_META ?
            PageMetaIO.VERSIONS.forPage(buf) :
            PagePartitionMetaIO.VERSIONS.forPage(buf);

        io.setTreeRoot(buf, treeRoot);
        io.setReuseListRoot(buf, reuseListRoot);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.META_PAGE_UPDATE_ROOTS;
    }
}
