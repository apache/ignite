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

package org.apache.ignite.internal.processors.cache;

import java.util.Objects;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

/**
 * A unified container for common, typical cache entry predicates.
 * <p>
 * The values test is CacheEntryPredicateAdapterMessageTest.
 */
public class CacheEntryPredicateAdapter implements CacheEntryPredicate {
    /** */
    private static final long serialVersionUID = 4647110502545358709L;

    /** */
    public static final CacheEntryPredicateAdapter ALWAYS_FALSE = new CacheEntryPredicateAdapter(PredicateType.ALWAYS_FALSE);

    /** */
    protected transient boolean locked;

    /** */
    @GridToStringInclude
    @Order(value = 0, method = "code", asType = "byte")
    private PredicateType type;

    /** */
    @GridToStringInclude
    @Order(value = 1, method = "value")
    @Nullable private CacheObject val;

    /** */
    public CacheEntryPredicateAdapter() {
        type = PredicateType.OTHER;
    }

    /** */
    public CacheEntryPredicateAdapter(PredicateType type) {
        assert type != null;

        this.type = type;
    }

    /** */
    public CacheEntryPredicateAdapter(@Nullable CacheObject val) {
        this.type = PredicateType.VALUE;

        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public void entryLocked(boolean locked) {
        this.locked = locked;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 98;
    }

    /** */
    public PredicateType type() {
        return type;
    }

    /**
     * @param entry Entry.
     * @return Value.
     */
    @Nullable private CacheObject peekVisibleValue(GridCacheEntryEx entry) {
        return locked ? entry.rawGet() : entry.peekVisibleValue();
    }

    /** {@inheritDoc} */
    @Override public boolean apply(GridCacheEntryEx e) {
        switch (type) {
            case VALUE: {
                CacheObject val = peekVisibleValue(e);

                if (this.val == null && val == null)
                    return true;

                if (this.val == null || val == null)
                    return false;

                GridCacheContext<?, ?> cctx = e.context();

                if (this.val instanceof BinaryObject && val instanceof BinaryObject)
                    return Objects.equals(val, this.val);

                Object thisVal = CU.value(this.val, cctx, false);
                Object cacheVal = CU.value(val, cctx, false);

                if (thisVal.getClass().isArray())
                    return Objects.deepEquals(thisVal, cacheVal);

                return Objects.equals(thisVal, cacheVal);
            }

            case HAS_VALUE:
                return peekVisibleValue(e) != null;

            case HAS_NO_VALUE:
                return peekVisibleValue(e) == null;

            case ALWAYS_FALSE:
                return false;
        }

        throw new IllegalStateException("Unknown cache entry predicate type: " + type);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        if (type == PredicateType.VALUE)
            val.finishUnmarshal(ctx.cacheObjectContext(), ldr);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheContext ctx) throws IgniteCheckedException {
        if (type == PredicateType.VALUE)
            val.prepareMarshal(ctx.cacheObjectContext());
    }

    /** */
    public @Nullable CacheObject value() {
        return val;
    }

    /** */
    public void value(@Nullable CacheObject val) {
        this.val = val;
    }

    /** */
    public byte code() {
        assert type != null;

        return (byte)type.ordinal();
    }

    /** */
    public void code(byte code) {
        type = code < 0 || code >= PredicateType.values().length ? PredicateType.OTHER : PredicateType.values()[code];
    }

    /** Common predicate type. */
    public enum PredicateType {
        /** Other custom predicate. */
        OTHER,
        /** Entry has certain equal value. */
        VALUE,
        /** Entry has any value. */
        HAS_VALUE,
        /** Entry has no value. */
        HAS_NO_VALUE,
        /** Is always false. */
        ALWAYS_FALSE
    }
}
