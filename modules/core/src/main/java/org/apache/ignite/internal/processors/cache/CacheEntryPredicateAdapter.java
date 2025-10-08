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

/** A unified container for common cache entry predicates. */
public class CacheEntryPredicateAdapter implements CacheEntryPredicate {
    /** */
    private static final long serialVersionUID = 4647110502545358709L;

    /** */
    public static final CacheEntryPredicateAdapter ALWAYS_FALSE = new CacheEntryPredicateAdapter(PredicateType.ALWAYS_FALSE);

    /** */
    protected transient boolean locked;

    /** */
    @GridToStringInclude
    @Order(value = 0, method = "typeEncoded", asType = "short")
    private PredicateType type;

    /** */
    @GridToStringInclude
    @Order(1)
    @Nullable private CacheObject val;

    /** */
    public CacheEntryPredicateAdapter() {
        type = PredicateType.OTHER;
    }

    /** */
    public CacheEntryPredicateAdapter(PredicateType type) {
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
    @Nullable protected CacheObject peekVisibleValue(GridCacheEntryEx entry) {
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
    public @Nullable CacheObject val() {
        return val;
    }

    /** */
    public void val(@Nullable CacheObject val) {
        this.val = val;
    }

    /** */
    public short typeEncoded() {
        switch (type) {
            case OTHER: return 1;
            case VALUE: return 2;
            case HAS_VALUE: return 3;
            case HAS_NO_VALUE: return 4;
            case ALWAYS_FALSE: return 5;
        }

        throw new IllegalStateException("Unknown cache entry predicate type: " + type);
    }

    /** */
    public void typeEncoded(short typeVal) {
        switch (typeVal) {
            case 0:
            case 1:
                type = PredicateType.OTHER;
                break;
            case 2: type = PredicateType.VALUE; break;
            case 3: type = PredicateType.HAS_VALUE; break;
            case 4: type = PredicateType.HAS_NO_VALUE; break;
            case 5: type = PredicateType.ALWAYS_FALSE; break;
            default:
                throw new IllegalStateException("Unknown cache entry predicate type value: " + typeVal);
        }
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
