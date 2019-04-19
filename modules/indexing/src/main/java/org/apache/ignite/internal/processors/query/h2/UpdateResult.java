/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Update result - modifications count and keys to re-run query with, if needed.
 */
public final class UpdateResult {
    /** Result to return for operations that affected 1 item - mostly to be used for fast updates and deletes. */
    public static final UpdateResult ONE = new UpdateResult(1, X.EMPTY_OBJECT_ARRAY);

    /** Result to return for operations that affected 0 items - mostly to be used for fast updates and deletes. */
    public static final UpdateResult ZERO = new UpdateResult(0, X.EMPTY_OBJECT_ARRAY);

    /** Number of processed items. */
    private final long cnt;

    /** Keys that failed to be updated or deleted due to concurrent modification of values. */
    private final Object[] errKeys;

    /**
     * Constructor.
     *
     * @param cnt Updated rows count.
     * @param errKeys Array of erroneous keys.
     */
    public @SuppressWarnings("ConstantConditions") UpdateResult(long cnt, Object[] errKeys) {
        this.cnt = cnt;
        this.errKeys = U.firstNotNull(errKeys, X.EMPTY_OBJECT_ARRAY);
    }

    /**
     * @return Update counter.
     */
    public long counter() {
       return cnt;
    }

    /**
     * @return Error keys.
     */
    public Object[] errorKeys() {
        return errKeys;
    }
}
