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

package org.apache.ignite.internal.processors.cache.tree.mvcc.search;

import org.apache.ignite.internal.processors.cache.KeyCacheObject;

/**
 * MVCC search row which contains a link. Now used only for cleanup purposes.
 */
public class MvccLinkAwareSearchRow extends MvccSearchRow {
    /** */
    private final long link;

    /**
     * @param cacheId Cache ID.
     * @param key Key.
     * @param crdVer Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     * @param link Link.
     */
    public MvccLinkAwareSearchRow(int cacheId, KeyCacheObject key, long crdVer, long mvccCntr, int mvccOpCntr, long link) {
        super(cacheId, key, crdVer, mvccCntr, mvccOpCntr);

        assert link != 0L;

        this.link = link;
    }

    /** {@inheritDoc} */
    @Override public long link() {
        return link;
    }
}
