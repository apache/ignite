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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.util.typedef.T2;
import org.jetbrains.annotations.Nullable;

/**
 * Partition destroy queue.
 */
public class PartitionDestroyQueue {
    /** */
    private final ConcurrentMap<T2<Integer, Integer>, PartitionDestroyRequest> pendingReqs =
        new ConcurrentHashMap<>();

    /**
     * @param grpCtx Group context.
     * @param partId Partition ID to destroy.
     */
    public void addDestroyRequest(@Nullable CacheGroupContext grpCtx, int grpId, int partId) {
        PartitionDestroyRequest req = new PartitionDestroyRequest(grpId, partId);

        PartitionDestroyRequest old = pendingReqs.putIfAbsent(new T2<>(grpId, partId), req);

        assert old == null || grpCtx == null : "Must wait for old destroy request to finish before adding a new one "
            + "[grpId=" + grpId
            + ", grpName=" + grpCtx.cacheOrGroupName()
            + ", partId=" + partId + ']';
    }

    /**
     * @param destroyId Destroy ID.
     * @return Destroy request to complete if was not concurrently cancelled.
     */
    private PartitionDestroyRequest beginDestroy(T2<Integer, Integer> destroyId) {
        PartitionDestroyRequest rmvd = pendingReqs.remove(destroyId);

        return rmvd == null ? null : rmvd.beginDestroy() ? rmvd : null;
    }

    /**
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @return Destroy request to wait for if destroy has begun.
     */
    public PartitionDestroyRequest cancelDestroy(int grpId, int partId) {
        PartitionDestroyRequest rmvd = pendingReqs.remove(new T2<>(grpId, partId));

        return rmvd == null ? null : !rmvd.cancel() ? rmvd : null;
    }

    /**
     * @return Pending reqs.
     */
    public ConcurrentMap<T2<Integer, Integer>, PartitionDestroyRequest> pendingReqs() {
        return pendingReqs;
    }
}
