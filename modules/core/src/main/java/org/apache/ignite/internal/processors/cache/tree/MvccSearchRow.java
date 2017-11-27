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

package org.apache.ignite.internal.processors.cache.tree;

import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class MvccSearchRow extends SearchRow {
    /** */
    private long crdVer;

    /** */
    private long mvccCntr;

    /**
     * @param cacheId Cache ID.
     * @param key Key.
     * @param crdVer Coordinator version.
     * @param mvccCntr Mvcc counter.
     */
    public MvccSearchRow(int cacheId, KeyCacheObject key, long crdVer, long mvccCntr) {
        super(cacheId, key);

        this.crdVer = crdVer;
        this.mvccCntr = mvccCntr;
    }

    /** {@inheritDoc} */
    @Override public long mvccCoordinatorVersion() {
        return crdVer;
    }

    /** {@inheritDoc} */
    @Override public long mvccCounter() {
        return mvccCntr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccSearchRow.class, this);
    }
}
