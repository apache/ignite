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

package org.apache.ignite.internal.processors.query.h2.dml;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.sql.SQLException;

/**
 * Result of processing an individual page with {@link IgniteCache#invokeAll} including error details, if any.
 */
public final class DmlPageProcessingResult {
    /** Number of successfully processed items. */
    private final long cnt;

    /** Keys that failed to be updated or deleted due to concurrent modification of values. */
    private final Object[] errKeys;

    /** Chain of exceptions corresponding to failed keys. Null if no keys yielded an exception. */
    private final SQLException ex;

    /** */
    public DmlPageProcessingResult(long cnt, Object[] errKeys, @Nullable SQLException ex) {
        this.cnt = cnt;
        this.errKeys = U.firstNotNull(errKeys, X.EMPTY_OBJECT_ARRAY);
        this.ex = ex;
    }

    /**
     * @return Number of successfully processed items.
     */
    public long count() {
        return cnt;
    }

    /**
     * @return Error keys.
     */
    public Object[] errorKeys() {
        return errKeys;
    }

    /**
     * @return Error.
     */
    @Nullable public SQLException error() {
        return ex;
    }
}
