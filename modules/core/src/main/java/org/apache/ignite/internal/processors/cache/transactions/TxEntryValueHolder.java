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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.CREATE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.DELETE;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.NOOP;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.READ;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.UPDATE;

/**
 * Auxiliary class to hold value, value-has-been-set flag, value update operation, value bytes.
 */
public class TxEntryValueHolder implements Message {
    /** Stored value. */
    @Order(value = 0, method = "storedValue")
    @GridToStringInclude(sensitive = true)
    private @Nullable CacheObject val;

    /** Cache operation. */
    @Order(value = 1, method = "operation")
    @GridToStringInclude
    private GridCacheOperation op = NOOP;

    /** Flag indicating that value has been set for write. */
    @Order(value = 2, method = "hasWriteValue")
    @GridToStringExclude
    private boolean hasWriteVal;

    /** Flag indicating that value has been set for read. */
    @GridToStringExclude
    private boolean hasReadVal;

    /**
     * @param op Cache operation.
     * @param val Value.
     * @param hasWriteVal Write value presence flag.
     * @param hasReadVal Read value presence flag.
     */
    public void value(GridCacheOperation op, CacheObject val, boolean hasWriteVal, boolean hasReadVal) {
        if (hasReadVal && this.hasWriteVal)
            return;

        this.op = op;
        this.val = val;

        this.hasWriteVal = hasWriteVal || op == CREATE || op == UPDATE || op == DELETE;
        this.hasReadVal = hasReadVal || op == READ;
    }

    /**
     * @return {@code True} if has read or write value.
     */
    public boolean hasValue() {
        return hasWriteVal || hasReadVal;
    }

    /**
     * Gets stored value.
     *
     * @return Value.
     */
    public CacheObject value() {
        return val;
    }

    /**
     * @param val Stored value.
     */
    public void value(@Nullable CacheObject val) {
        this.val = val;
    }

    /**
     * Used only in serializer.
     *
     * @return Stored value or null.
     */
    public @Nullable CacheObject storedValue() {
        return hasWriteVal ? val : null;
    }

    /**
     * Used only in serializer.
     *
     * @param val Stored value.
     */
    public void storedValue(@Nullable CacheObject val) {
        this.val = val;
    }

    /**
     * @return Cache operation.
     */
    public GridCacheOperation operation() {
        return op;
    }

    /**
     * @param op Cache operation.
     */
    public void operation(GridCacheOperation op) {
        this.op = op;
    }

    /**
     * @return Flag indicating that value has been set for write.
     */
    public boolean hasWriteValue() {
        return hasWriteVal;
    }

    /**
     * @param hasWriteVal Flag indicating that value has been set for write.
     */
    public void hasWriteValue(boolean hasWriteVal) {
        this.hasWriteVal = hasWriteVal;
    }

    /**
     * @return Flag indicating that value has been set for read.
     */
    public boolean hasReadValue() {
        return hasReadVal;
    }

    /**
     * @param ctx Cache context.
     * @throws IgniteCheckedException If marshaling failed.
     */
    public void marshal(GridCacheContext<?, ?> ctx) throws IgniteCheckedException {
        if (hasWriteVal && val != null)
            val.prepareMarshal(ctx.cacheObjectContext());
    }

    /**
     * @param ctx Cache context.
     * @param ldr Class loader.
     * @throws IgniteCheckedException If unmarshalling failed.
     */
    public void unmarshal(CacheObjectValueContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        if (hasWriteVal && val != null)
            val.finishUnmarshal(ctx, ldr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxEntryValueHolder.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 101;
    }
}
