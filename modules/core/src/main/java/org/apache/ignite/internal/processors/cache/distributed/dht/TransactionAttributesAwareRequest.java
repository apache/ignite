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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxPrepareRequest;

/** Wraps transaction prepare request with application attributes. */
public class TransactionAttributesAwareRequest extends GridCacheMessage {
    /** */
    public static final short TYPE_CODE = 181;

    /** Original transaction prepare message. */
    @Order(3)
    private GridDistributedTxPrepareRequest payload;

    /** Application attributes. */
    @Order(value = 4, method = "applicationAttributes")
    private Map<String, String> appAttrs;

    /** */
    public TransactionAttributesAwareRequest() {
    }

    /** */
    public TransactionAttributesAwareRequest(
        GridDistributedTxPrepareRequest payload,
        Map<String, String> appAttrs
    ) {
        this.payload = payload;
        this.appAttrs = appAttrs;
    }

    /** @return Original update message. */
    public GridDistributedTxPrepareRequest payload() {
        return payload;
    }

    /** @param payload Original update message. */
    public void payload(GridDistributedTxPrepareRequest payload) {
        this.payload = payload;
    }

    /** @return Application attributes. */
    public Map<String, String> applicationAttributes() {
        return appAttrs;
    }

    /** @param appAttrs Application attributes. */
    public void applicationAttributes(Map<String, String> appAttrs) {
        this.appAttrs = appAttrs;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        payload.prepareMarshal(ctx);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr) throws IgniteCheckedException {
        payload.finishUnmarshal(ctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public int handlerId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean cacheGroupMessage() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
