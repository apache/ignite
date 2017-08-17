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

/**
 * Base class for Ignite cache queries which can merge tables.
 */
public abstract class MergeQuery<R> extends Query<R> {
    /** Maximum number of SQL result rows which can be fetched into a merge table. */
    private int sqlMergeTblMaxSize;

    /**
     * Number of SQL result rows that will be fetched into a merge table at once before applying binary search for the
     * bounds.
     */
    private int sqlMergeTblPrefetchSize;

    /**
     * Gets property controlling maximum number of SQL result rows which can be fetched into a merge table. If there are
     * less rows than this threshold then multiple passes throw a table will be possible, otherwise only one pass (e.g.
     * only result streaming is possible).
     *
     * @return Maximum number of SQL result rows which can be fetched into a merge table.
     */
    public int getSqlMergeTableMaxSize() {
        return sqlMergeTblMaxSize;
    }

    /**
     * Sets property controlling maximum number of SQL result rows which can be fetched into a merge table. If there are
     * less rows than this threshold then multiple passes throw a table will be possible, otherwise only one pass (e.g.
     * only result streaming is possible).
     *
     * @param sqlMergeTblMaxSize Maximum number of SQL result rows which can be fetched into a merge table. Must be
     * positive and greater than {@link MergeQuery#sqlMergeTblPrefetchSize}.
     * @return {@code this} for chaining.
     */
    public MergeQuery<R> setSqlMergeTableMaxSize(int sqlMergeTblMaxSize) {
        this.sqlMergeTblMaxSize = sqlMergeTblMaxSize;

        return this;
    }

    /**
     * Gets number of SQL result rows that will be fetched into a merge table at once before applying binary search for
     * the bounds.
     *
     * @return Number of SQL result rows that will be fetched into a merge table.
     */
    public int getSqlMergeTablePrefetchSize() {
        return sqlMergeTblPrefetchSize;
    }

    /**
     * Sets number of SQL result rows that will be fetched into a merge table at once before applying binary search for
     * the bounds.
     *
     * @param sqlMergeTblPrefetchSize Number of SQL result rows that will be fetched into a merge table. Must be
     * positive and power of 2 and less than {@link MergeQuery#sqlMergeTblMaxSize}.
     * @return {@code this} for chaining.
     */
    public MergeQuery<R> setSqlMergeTablePrefetchSize(int sqlMergeTblPrefetchSize) {
        this.sqlMergeTblPrefetchSize = sqlMergeTblPrefetchSize;

        return this;
    }
}
