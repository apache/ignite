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

package org.apache.ignite.cache.query;

public interface QueryHistoryMetrics {
    /**
     * @return Textual representation of query.
     */
    public String query();

    /**
     * @return Schema.
     */
    public String schema();

    /**
     * @return {@code true} For query with enabled local flag.
     */
    public boolean local();

    /**
     * Gets total number execution of query.
     *
     * @return Number of executions.
     */
    public int executions();

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
     * Gets latest query start time.
     *
     * @return Latest time query was stared.
     */
    public long lastStartTime();
}
