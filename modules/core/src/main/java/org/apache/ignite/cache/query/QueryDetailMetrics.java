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

package org.apache.ignite.cache.query;

import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Query metrics aggregated by query type and its textual representation.
 *
 * Query detail metrics could be enabled via {@link CacheConfiguration#setQueryDetailMetricsSize(int)} method.
 */
public interface QueryDetailMetrics {
    /**
     * @return Query type.
     */
    public String queryType();

    /**
     * @return Textual representation of query.
     */
    public String query();

    /**
     * @return Cache where query was executed.
     */
    public String cache();

    /**
     * Gets total number execution of query.
     *
     * @return Number of executions.
     */
    public int executions();

    /**
     * Gets number of completed execution of query.
     *
     * @return Number of completed executions.
     */
    public int completions();

    /**
     * Gets number of times a query execution failed.
     *
     * @return Number of times a query execution failed.
     */
    public int failures();

    /**
     * Gets minimum execution time of query.
     *
     * @return Minimum execution time of query.
     */
    public long minimumTime();

    /**
     * Gets maximum execution time of query.
     *
     * @return Maximum execution time of query.
     */
    public long maximumTime();

    /**
     * Gets average execution time of query.
     *
     * @return Average execution time of query.
     */
    public double averageTime();

    /**
     * Gets total time of all query executions.
     *
     * @return Total time of all query executions.
     */
    public long totalTime();

    /**
     * Gets latest query start time.
     *
     * @return Latest time query was stared.
     */
    public long lastStartTime();
}
