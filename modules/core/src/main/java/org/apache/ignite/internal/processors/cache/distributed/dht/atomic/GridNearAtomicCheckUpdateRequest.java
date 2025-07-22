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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class GridNearAtomicCheckUpdateRequest extends GridCacheIdMessage {
    /** Cache message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** */
    private GridNearAtomicAbstractUpdateRequest updateReq;

    /** */
    @Order(value = 0, method = "partition")
    private int partId;

    /** */
    @Order(value = 1, method = "futureId")
    private long futId;

    /**
     *
     */
    public GridNearAtomicCheckUpdateRequest() {
        // No-op.
    }

    /**
     * @param updateReq Related update request.
     */
    GridNearAtomicCheckUpdateRequest(GridNearAtomicAbstractUpdateRequest updateReq) {
        assert updateReq != null && updateReq.fullSync() : updateReq;

        this.updateReq = updateReq;
        this.cacheId = updateReq.cacheId();
        this.partId = updateReq.partition();
        this.futId = updateReq.futureId();

        assert partId >= 0;
    }

    /**
     * @return Future ID on near node.
     */
    public final long futureId() {
        return futId;
    }

    /**
     * @param futId Future ID on near node.
     */
    public void futureId(long futId) {
        this.futId = futId;
    }


    /**
     * @return Related update request.
     */
    GridNearAtomicAbstractUpdateRequest updateRequest() {
        return updateReq;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return partId;
    }

    /**
     * @param partId Partition ID this message is targeted to or {@code -1} if it cannot be determined.
     */
    public void partition(int partId) {
        this.partId = partId;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -50;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicCheckUpdateRequest.class, this);
    }
}
