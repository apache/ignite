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

import org.apache.ignite.cache.query.FieldsQueryCursor;

import java.util.List;

/**
 * Command execution result.
 */
public class CommandResult {
    /** Cursor. */
    private final FieldsQueryCursor<List<?>> cur;

    /** Whether running query should be unregistered. */
    private final boolean unregisterRunningQry;

    /**
     * Constructor.
     *
     * @param cur Cursor.
     * @param unregisterRunningQry Whether running query should be unregistered.
     */
    public CommandResult(FieldsQueryCursor<List<?>> cur, boolean unregisterRunningQry) {
        this.cur = cur;
        this.unregisterRunningQry = unregisterRunningQry;
    }

    /**
     * @return Cursor.
     */
    public FieldsQueryCursor<List<?>> cursor() {
        return cur;
    }

    /**
     * @return Whether running query should be unregistered.
     */
    public boolean unregisterRunningQuery() {
        return unregisterRunningQry;
    }
}
