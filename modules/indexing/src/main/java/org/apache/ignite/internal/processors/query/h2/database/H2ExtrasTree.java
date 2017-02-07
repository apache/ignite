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

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.query.h2.database.io.H2Extras32InnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2Extras32LeafIO;

/**
 */
public abstract class H2ExtrasTree extends H2Tree {

    /** */
    private final List<FastIndexHelper> fastIdxs;

    /**
     * @param name Tree name.
     * @param reuseList Reuse list.
     * @param cacheId Cache ID.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     * @param rowStore Row data store.
     * @param metaPageId Meta page ID.
     * @param initNew Initialize new index.
     * @throws IgniteCheckedException If failed.
     */
    public H2ExtrasTree(
        String name,
        ReuseList reuseList,
        int cacheId,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        AtomicLong globalRmvId,
        H2RowFactory rowStore,
        long metaPageId,
        boolean initNew,
        List<FastIndexHelper> fastIdxs
    ) throws IgniteCheckedException {
        super(
            name,
            reuseList,
            cacheId,
            pageMem,
            wal,
            globalRmvId,
            rowStore,
            metaPageId,
            initNew,
            H2Extras32InnerIO.VERSIONS,
            H2Extras32LeafIO.VERSIONS);

        this.fastIdxs = fastIdxs;
    }

    /**
     * @return FastIndexHelper list.
     */
    public List<FastIndexHelper> fastIdxs() {
        return fastIdxs;
    }
}
