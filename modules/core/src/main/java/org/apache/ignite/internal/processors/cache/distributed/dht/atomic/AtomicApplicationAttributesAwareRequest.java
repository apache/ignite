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

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;

/** Wraps atomic updates with application attributes. */
public class AtomicApplicationAttributesAwareRequest extends GridCacheIdMessage {
    /** */
    public static final short TYPE_CODE = 180;

    /** Original update message. */
    @Order(4)
    GridNearAtomicAbstractUpdateRequest payload;

    /** Application attributes. */
    @Order(5)
    Map<String, String> appAttrs;

    /** */
    public AtomicApplicationAttributesAwareRequest() {
    }

    /** */
    public AtomicApplicationAttributesAwareRequest(
        GridNearAtomicAbstractUpdateRequest payload,
        Map<String, String> appAttrs
    ) {
        this.payload = payload;
        this.appAttrs = appAttrs;
        cacheId = payload.cacheId();
    }

    /** @return Original update message. */
    public GridNearAtomicAbstractUpdateRequest payload() {
        return payload;
    }

    /** @param payload Original update message. */
    public void payload(GridNearAtomicAbstractUpdateRequest payload) {
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
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
