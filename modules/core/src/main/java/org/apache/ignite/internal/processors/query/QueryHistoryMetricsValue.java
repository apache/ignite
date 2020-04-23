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

/**
 * Immutable query metrics.
 */
class QueryHistoryMetricsValue {
    /** Number of executions. */
    private final long execs;

    /** Number of failures. */
    private final long failures;

    /** Minimum time of execution. */
    private final long minTime;

    /** Maximum time of execution. */
    private final long maxTime;

    /** Last start time of execution. */
    private final long lastStartTime;

    /**
     * @param execs Number of executions.
     * @param failures Number of failure.
     * @param minTime Min time of execution.
     * @param maxTime Max time of execution.
     * @param lastStartTime Last start time of execution.
     */
    public QueryHistoryMetricsValue(long execs, long failures, long minTime, long maxTime, long lastStartTime) {
        this.execs = execs;
        this.failures = failures;
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.lastStartTime = lastStartTime;
    }

   /**
     * Gets total number execution of query.
     *
     * @return Number of executions.
     */
    public long execs() {
        return execs;
    }

    /**
     * Gets number of times a query execution failed.
     *
     * @return Number of times a query execution failed.
     */
    public long failures() {
        return failures;
    }

    /**
     * Gets minimum execution time of query.
     *
     * @return Minimum execution time of query.
     */
    public long minTime() {
        return minTime;
    }

    /**
     * Gets maximum execution time of query.
     *
     * @return Maximum execution time of query.
     */
    public long maxTime() {
        return maxTime;
    }

    /**
     * Gets latest query start time.
     *
     * @return Latest time query was stared.
     */
    public long lastStartTime() {
        return lastStartTime;
    }
}
