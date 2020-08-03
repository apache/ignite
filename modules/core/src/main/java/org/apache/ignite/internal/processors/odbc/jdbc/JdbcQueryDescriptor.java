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

package org.apache.ignite.internal.processors.odbc.jdbc;

import org.apache.ignite.internal.processors.query.GridQueryCancel;

/**
 * JDBC query descriptor used for appropriate query cancellation.
 */
public class JdbcQueryDescriptor {
    /** Hook for cancellation. */
    private final GridQueryCancel cancelHook;

    /** Canceled flag. */
    private boolean canceled;

    /** Usage count of given descriptor. */
    private int usageCnt;

    /** Execution started flag.  */
    private boolean executionStarted;

    /**
     * Constructor.
     */
    public JdbcQueryDescriptor() {
        cancelHook = new GridQueryCancel();
    }

    /**
     * @return Hook for cancellation.
     */
    public GridQueryCancel cancelHook() {
        return cancelHook;
    }

    /**
     * @return True if descriptor was marked as canceled.
     */
    public boolean isCanceled() {
        return canceled;
    }

    /**
     * Marks descriptor as canceled.
     */
    public void markCancelled() {
        canceled = true;
    }

    /**
     * Increments usage count.
     */
    public void incrementUsageCount() {
        usageCnt++;

        executionStarted = true;
    }

    /**
     * Descrements usage count.
     */
    public void decrementUsageCount() {
        usageCnt--;
    }

    /**
     * @return Usage count.
     */
    public int usageCount() {
        return usageCnt;
    }

    /**
     * @return True if execution was started, false otherwise.
     */
    public boolean isExecutionStarted() {
        return executionStarted;
    }
}
