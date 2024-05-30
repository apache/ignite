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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/** */
public abstract class AbstractSnapshotPartsSenderFuture<T> extends AbstractSnapshotCacheAffectingFuture<T> {
    /** Snapshot data sender. */
    @GridToStringExclude
    protected final SnapshotSender snpSndr;

    /** Partition to be processed. */
    protected final Map<Integer, Set<Integer>> parts;

    /**
     * Ctor.
     *
     * @param sharedCacheCtx Shared cache context.
     * @param log Logger.
     * @param srcNodeId Node id which cause snapshot task creation.
     * @param reqId Snapshot operation request ID.
     * @param snpName Unique identifier of snapshot process.
     * @param snpSndr Factory which produces snapshot receiver instance.
     * @param parts Partition to be processed.
     */
    protected AbstractSnapshotPartsSenderFuture(
        GridCacheSharedContext<?, ?> sharedCacheCtx,
        IgniteLogger log,
        UUID srcNodeId,
        UUID reqId,
        String snpName,
        SnapshotSender snpSndr,
        Map<Integer, Set<Integer>> parts
    ) {
        super(sharedCacheCtx, log, srcNodeId, reqId, snpName);

        this.snpSndr = snpSndr;
        this.parts = parts;
    }

    /** {@inheritDoc} */

    @Override public Set<Integer> affectedCacheGroups() {
        return parts.keySet();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AbstractSnapshotPartsSenderFuture.class, this);
    }
}
