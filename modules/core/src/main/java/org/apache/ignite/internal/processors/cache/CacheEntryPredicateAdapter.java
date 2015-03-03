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

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.nio.*;

/**
 *
 */
public abstract class CacheEntryPredicateAdapter implements CacheEntryPredicate {
    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheContext ctx) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        assert false : this;

        return 0;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        assert false : this;

        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        assert false : this;

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        assert false : this;

        return false;
    }

    /**
     * @param e Entry.
     * @return {@code True} if given entry has value.
     */
    protected boolean hasValue(GridCacheEntryEx e) {
        try {
            if (e.hasValue())
                return true;

            GridCacheContext cctx = e.context();

            if (cctx.transactional()) {
                IgniteInternalTx tx = cctx.tm().userTx();

                if (tx != null)
                    return tx.peek(cctx, false, e.key(), null) != null;
            }

            return false;
        }
        catch (GridCacheFilterFailedException err) {
            assert false;

            err.printStackTrace();

            return false;
        }
    }
}
