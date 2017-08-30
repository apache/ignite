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

package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class TxMvccVersion implements Comparable<TxMvccVersion> {
    /** */
    public static final long COUNTER_NA = 0L;

    /** */
    private final long topVer;

    /** */
    private final long cntr;

    /** */
    private final GridCacheVersion txId;

    /**
     * @param topVer Topology version.
     * @param cntr Coordinator counter.
     * @param txId Transaction ID.
     */
    public TxMvccVersion(long topVer, long cntr, GridCacheVersion txId) {
        assert topVer > 0 : topVer;
        assert cntr != COUNTER_NA;
        assert txId != null;

        this.topVer = topVer;
        this.cntr = cntr;
        this.txId = txId;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull TxMvccVersion other) {
        int cmp = Long.compare(topVer, other.topVer);

        if (cmp != 0)
            return cmp;

        return Long.compare(cntr, other.cntr);
    }

    /**
     * @return Coordinators topology version.
     */
    public long topologyVersion() {
        return topVer;
    }

    /**
     * @return Counters.
     */
    public long counter() {
        return cntr;
    }

    /**
     * @return Transaction ID.
     */
    public GridCacheVersion txId() {
        return txId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxMvccVersion.class, this);
    }
}
