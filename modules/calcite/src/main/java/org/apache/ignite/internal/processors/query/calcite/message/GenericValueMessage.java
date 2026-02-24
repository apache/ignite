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

package org.apache.ignite.internal.processors.query.calcite.message;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public final class GenericValueMessage implements ValueMessage {
    /** */
    private Object val;

    /** */
    @Order(0)
    byte[] serialized;

    /** */
    public GenericValueMessage() {

    }

    /** */
    public GenericValueMessage(Object val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public Object value() {
        return val;
    }

    /**
     * @return Serialized value.
     */
    public byte[] serialized() {
        return serialized;
    }

    /**
     * @param serialized Serialized value.
     */
    public void serialized(byte[] serialized) {
        this.serialized = serialized;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        if (val != null && serialized == null)
            serialized = U.marshal(ctx, val);
    }

    /** {@inheritDoc} */
    @Override public void prepareUnmarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        if (serialized != null && val == null)
            val = U.unmarshal(ctx, serialized, U.resolveClassLoader(ctx.gridConfig()));
    }

    /** {@inheritDoc} */
    @Override public MessageType type() {
        return MessageType.GENERIC_VALUE_MESSAGE;
    }
}
