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

/** A unified container for common, typical cache entry predicates. */
public class CacheEntryPredicateAdapter implements CacheEntryPredicate {
    /** */
    private static final long serialVersionUID = 4647110502545358709L;

    /** */
    protected transient boolean locked;

    /** */
    @GridToStringInclude
    @Order(0)
    CacheEntryPredicateType type;

    /** */
    @GridToStringInclude
    @Order(1)
    @Nullable CacheObject val;

    /** */
    public CacheEntryPredicateAdapter() {
        type = CacheEntryPredicateType.OTHER;
    }

    /** */
    public CacheEntryPredicateAdapter(CacheEntryPredicateType type) {
        assert type != null;

        this.type = type;
    }

    /** */
    public CacheEntryPredicateAdapter(@Nullable CacheObject val) {
        type = CacheEntryPredicateType.VALUE;

        this.val = val;
    }

    /** {@inheritDoc} */
    @Override public void entryLocked(boolean locked) {
        this.locked = locked;
    }

    /** */
    public CacheEntryPredicateType type() {
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
        if (type == CacheEntryPredicateType.VALUE)
            val.finishUnmarshal(ctx.cacheObjectContext(), ldr);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheContext ctx) throws IgniteCheckedException {
        if (type == CacheEntryPredicateType.VALUE)
            val.prepareMarshal(ctx.cacheObjectContext());
    }

}
