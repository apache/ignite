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
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;

/**
 * Split existing page.
 */
public class SplitExistingPageRecord extends PageDeltaRecord {
    /** */
    private int mid;

    /** */
    private long fwdId;

    /**
     * @param cacheId Cache ID.
     * @param pageId  Page ID.
     * @param mid Bisection index.
     * @param fwdId New forward page ID.
     */
    public SplitExistingPageRecord(int cacheId, long pageId, int mid, long fwdId) {
        super(cacheId, pageId);

        this.mid = mid;
        this.fwdId = fwdId;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(ByteBuffer buf) throws IgniteCheckedException {
        BPlusIO<?> io = PageIO.getBPlusIO(buf);

        io.splitExistingPage(buf, mid, fwdId);
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.BTREE_EXISTING_PAGE_SPLIT;
    }

    public int middleIndex() {
        return mid;
    }

    public long forwardId() {
        return fwdId;
    }
}
