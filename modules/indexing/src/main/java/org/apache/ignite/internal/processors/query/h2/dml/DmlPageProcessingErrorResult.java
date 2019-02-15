/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.query.h2.dml;

import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.cache.processor.EntryProcessor;
import java.sql.SQLException;

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
    @SuppressWarnings("ConstantConditions")
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
