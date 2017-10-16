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
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinatorVersion;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.cache.mvcc.CacheCoordinatorsProcessor.createVersionForRemovedValue;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheCoordinatorsProcessor.unmaskCoordinatorVersion;

/**
 *
 */
public class MvccRemoveRow extends MvccUpdateRow {
    /**
     * @param key Key.
     * @param mvccVer Mvcc version.
     * @param part Partition.
     * @param cacheId Cache ID.
     */
    public MvccRemoveRow(
        KeyCacheObject key,
        MvccCoordinatorVersion mvccVer,
        boolean needOld,
        int part,
        int cacheId) {
        super(key, null, null, 0L, mvccVer, needOld, part, cacheId);
    }

    /** {@inheritDoc} */
    @Override public long mvccCoordinatorVersion() {
        return createVersionForRemovedValue(super.mvccCoordinatorVersion());
    }

    /** {@inheritDoc} */
    @Override protected long unmaskedCoordinatorVersion() {
        return unmaskCoordinatorVersion(super.mvccCoordinatorVersion());
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccRemoveRow.class, this, "super", super.toString());
    }
}
