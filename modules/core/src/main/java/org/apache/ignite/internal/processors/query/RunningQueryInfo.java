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
 *
 */

package org.apache.ignite.internal.processors.query;

import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Represents information about running query.
 */
public class RunningQueryInfo {
    /** Unique query identifier. */
    private final UUID qryId;

    /** Query text. */
    private final String qry;

    /** Schema name. */
    private final String schemaName;

    /**
     *  Time of starting execution of query in milliseconds between the current time and midnight, January 1, 1970 UTC.
     */
    private final long startTime;

    /** Query cancel. */
    private final GridQueryCancel cancel;

    /** Running stage. */
    private final RunningStage stage;

    /**
     * Constructor.
     *
     * @param stage Stage of execution.
     * @param qryId Unique query identifier.
     * @param qry Query text.
     * @param schemaName Schema name.
     * @param cancel Query cancel.
     */
    public RunningQueryInfo(
        RunningStage stage,
        UUID qryId,
        String qry,
        String schemaName,
        GridQueryCancel cancel) {
        this.qryId = qryId;
        this.qry = qry;
        this.schemaName = schemaName;
        this.cancel = cancel;
        this.stage = stage;

        startTime = U.currentTimeMillis();
    }

    /**
     * Stage of query execution.
     */
    public RunningStage stage() {
        return stage;
    }

    /**
     * @return Query ID.
     */
    public UUID qryId() {
        return qryId;
    }

    /**
     * @return Query text.
     */
    public String query() {
        return qry;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Query start time.
     */
    public long startTime() {
        return startTime;
    }

    /**
     * Cancel query.
     */
    public void cancel() {
        if (cancel != null)
            cancel.cancel();
    }

    /**
     * @return {@code true} if query can be cancelled.
     */
    public boolean cancelable() {
        return cancel != null;
    }
}
