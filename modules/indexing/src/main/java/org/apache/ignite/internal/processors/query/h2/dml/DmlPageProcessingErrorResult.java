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

package org.apache.ignite.internal.processors.query.h2.dml;

import java.sql.SQLException;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Result of splitting keys whose processing resulted into an exception from those skipped by
 * logic of {@link EntryProcessor}s (most likely INSERT duplicates, or UPDATE/DELETE keys whose values
 * had been modified concurrently), counting and collecting entry processor exceptions.
 */
public final class DmlPageProcessingErrorResult {
    /** Keys that failed to be processed by {@link EntryProcessor} (not due to an exception). */
    private final Object[] errKeys;

    /** Number of entries whose processing resulted into an exception. */
    private final int cnt;

    /** Chain of exceptions corresponding to failed keys. Null if no keys yielded an exception. */
    private final SQLException ex;

    /** */
    public DmlPageProcessingErrorResult(@NotNull Object[] errKeys, SQLException ex, int exCnt) {
        errKeys = U.firstNotNull(errKeys, X.EMPTY_OBJECT_ARRAY);
        // When exceptions count must be zero, exceptions chain must be not null, and vice versa.
        assert exCnt == 0 ^ ex != null;

        this.errKeys = errKeys;
        this.cnt = exCnt;
        this.ex = ex;
    }

    /**
     * @return Number of entries whose processing resulted into an exception.
     */
    public int errorCount() {
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
    @Nullable
    public SQLException error() {
        return ex;
    }
}
