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
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MarshallableMessage;
import org.jetbrains.annotations.Nullable;

/** A unified container for common, typical cache entry predicates. */
public class CacheEntryPredicateAdapter implements CacheEntryPredicate, MarshallableMessage {
    /** */
    private static final long serialVersionUID = 4647110502545358709L;

    /** */
    public static final CacheEntryPredicateAdapter ALWAYS_FALSE = new CacheEntryPredicateAdapter(PredicateType.ALWAYS_FALSE);

    /** */
    protected transient boolean locked;

    /** */
    @GridToStringInclude
    private PredicateType type;

    /** Type value serialization holder. */
    @Order(0)
    protected transient byte code;

    /** */
    @GridToStringInclude
    @Order(1)
    @Nullable CacheObject val;

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
        type = PredicateType.VALUE;
        code = 1;

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
    public byte code() {
        return code;
    }

    /** */
    public void code(byte code) {
        this.code = code;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        switch (type) {
            case OTHER:
                code = 0;
                break;

            case VALUE:
                code = 1;
                break;

            case HAS_VALUE:
                code = 2;
                break;

            case HAS_NO_VALUE:
                code = 3;
                break;

            case ALWAYS_FALSE:
                code = 4;
                break;

            default:
                throw new IllegalArgumentException("Unknown cache entry predicate type: " + type);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        switch (code) {
            case 0:
                type = PredicateType.OTHER;
                break;

            case 1:
                type = PredicateType.VALUE;
                break;

            case 2:
                type = PredicateType.HAS_VALUE;
                break;

            case 3:
                type = PredicateType.HAS_NO_VALUE;
                break;

            case 4:
                type = PredicateType.ALWAYS_FALSE;
                break;

            default:
                throw new IllegalArgumentException("Unknown cache entry predicate type code: " + code);
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
