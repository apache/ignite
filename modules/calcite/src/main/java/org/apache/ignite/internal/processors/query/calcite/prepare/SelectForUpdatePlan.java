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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.cache.CacheEntry;
import org.jetbrains.annotations.Nullable;

/**
 * Query plan for {@code SELECT ... FOR UPDATE} statements.
 *
 * <p>Wraps an inner {@link MultiStepQueryPlan} whose SELECT list has {@code _KEY}, {@code _VAL},
 * and {@code _VER} columns of the tables participating in the query appended at the end. At execution time
 * the executor:
 * <ol>
 *   <li>Runs the inner plan and materialises the full result set.</li>
 *   <li>Extracts the hidden columns described by {@link #lockTargets} from every row.</li>
 *   <li>Creates {@link CacheEntry} to lock in the current transaction.</li>
 *   <li>Calls {@code cache.lockTxEntries(entries, waitMs)} to acquire pessimistic locks.</li>
 *   <li>Repeats the inner plan when a selected entry version changes before locking.</li>
 *   <li>Returns only the first {@link #userColumnCount} columns to the caller.</li>
 * </ol>
 */
public class SelectForUpdatePlan extends AbstractQueryPlan {
    /** Inner plan: SELECT with _KEY, _VAL, and _VER columns appended at the end. */
    private final MultiStepQueryPlan innerPlan;

    /** Number of user-visible result columns (total inner columns minus the appended _KEY, _VAL, and _VER). */
    private final int userColumnCount;

    /**
     * Lock-wait limit from the {@code FOR UPDATE} clause:
     * {@code null} = use transaction timeout, {@code 0L} = NOWAIT,
     * positive value = WAIT n seconds.
     */
    @Nullable private final Long waitSeconds;

    /** Tables whose rows must be locked and positions of their hidden columns in the inner result. */
    private final List<LockTarget> lockTargets;

    /** */
    public SelectForUpdatePlan(
        MultiStepQueryPlan innerPlan,
        int userColumnCount,
        @Nullable Long waitSeconds,
        List<LockTarget> lockTargets
    ) {
        super(innerPlan.query());

        this.innerPlan = innerPlan;
        this.userColumnCount = userColumnCount;
        this.waitSeconds = waitSeconds;
        this.lockTargets = Collections.unmodifiableList(new ArrayList<>(lockTargets));
    }

    /** Returns the inner plan that reads rows together with the appended columns. */
    public MultiStepQueryPlan innerPlan() {
        return innerPlan;
    }

    /** Number of user-visible result columns (the _KEY, _VAL, and _VER columns are not counted). */
    public int userColumnCount() {
        return userColumnCount;
    }

    /**
     * Returns the lock-wait limit: {@code null} = transaction timeout, {@code 0L} = NOWAIT,
     * positive = WAIT n seconds.
     */
    @Nullable public Long waitSeconds() {
        return waitSeconds;
    }

    /** Tables whose rows must be locked. */
    public List<LockTarget> lockTargets() {
        return lockTargets;
    }

    /** {@inheritDoc} */
    @Override public Type type() {
        return Type.FOR_UPDATE;
    }

    /** {@inheritDoc} */
    @Override public QueryPlan copy() {
        return new SelectForUpdatePlan(
            (MultiStepQueryPlan)innerPlan.copy(),
            userColumnCount,
            waitSeconds,
            lockTargets
        );
    }

    /** A table to lock and the position of its {@code _KEY} column in the inner result. */
    public static class LockTarget {
        /** SQL schema name. */
        private final String schemaName;

        /** SQL table name. */
        private final String tableName;

        /** Index of {@code _KEY}; {@code _VAL} and {@code _VER} immediately follow it. */
        private final int keyColumnIndex;

        /** */
        public LockTarget(String schemaName, String tableName, int keyColumnIndex) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.keyColumnIndex = keyColumnIndex;
        }

        /** SQL schema name. */
        public String schemaName() {
            return schemaName;
        }

        /** SQL table name. */
        public String tableName() {
            return tableName;
        }

        /** Index of the hidden {@code _KEY} column in the inner result. */
        public int keyColumnIndex() {
            return keyColumnIndex;
        }
    }
}
