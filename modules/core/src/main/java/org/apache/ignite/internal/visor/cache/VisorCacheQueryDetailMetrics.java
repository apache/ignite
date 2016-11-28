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

package org.apache.ignite.internal.visor.cache;

import java.io.Serializable;
import org.apache.ignite.cache.query.QueryDetailMetrics;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for cache query detail metrics.
 */
public class VisorCacheQueryDetailMetrics implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query type. */
    private String qryType;

    /** Textual query representation. */
    private String qry;

    /** Cache name. */
    private String cache;

    /** Number of executions. */
    private int execs;

    /** Number of completions executions. */
    private int completions;

    /** Number of failures. */
    private int failures;

    /** Minimum time of execution. */
    private long minTime;

    /** Maximum time of execution. */
    private long maxTime;

    /** Average time of execution. */
    private double avgTime;

    /** Sum of execution time of completions time. */
    private long totalTime;

    /** Sum of execution time of completions time. */
    private long lastStartTime;

    /**
     * @param m Cache query metrics.
     * @return Data transfer object for given cache metrics.
     */
    public VisorCacheQueryDetailMetrics from(QueryDetailMetrics m) {
        qryType = m.queryType();
        qry = m.query();
        cache = m.cache();

        execs = m.executions();
        completions = m.completions();
        failures = m.failures();

        minTime = m.minimumTime();
        maxTime = m.maximumTime();
        avgTime = m.averageTime();
        totalTime = m.totalTime();
        lastStartTime = m.lastStartTime();

        return this;
    }

    /**
     * @return Query type
     */
    public String getQueryType() {
        return qryType;
    }

    /**
     * @return Query type
     */
    public String getQuery() {
        return qry;
    }

    /**
     * @return Cache name where query was executed.
     */
    public String getCache() {
        return cache;
    }

    /**
     * @return Number of executions.
     */
    public int getExecutions() {
        return execs;
    }

    /**
     * @return Number of completed executions.
     */
    public int getCompletions() {
        return completions;
    }

    /**
     * @return Total number of times a query execution failed.
     */
    public int getFailures() {
        return failures;
    }

    /**
     * @return Minimum execution time of query.
     */
    public long getMinimumTime() {
        return minTime;
    }

    /**
     * @return Maximum execution time of query.
     */
    public long getMaximumTime() {
        return maxTime;
    }

    /**
     * @return Average execution time of query.
     */
    public double getAverageTime() {
        return avgTime;
    }

    /**
     * @return Total time of all query executions.
     */
    public long getTotalTime() {
        return totalTime;
    }

    /**
     * @return Latest time query was stared.
     */
    public long getLastStartTime() {
        return lastStartTime;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheQueryDetailMetrics.class, this);
    }
}
