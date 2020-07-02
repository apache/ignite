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

package org.apache.ignite.internal.processors.query.h2;

import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;

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
