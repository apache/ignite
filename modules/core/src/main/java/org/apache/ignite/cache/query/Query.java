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

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Main API for configuring and executing cache queries.
 *
 * Supported queries are:
 * <ul>
 * <li><b>SQL Fields query.</b>Provides SQL way with full syntax to access cache data.
 * See {@link SqlFieldsQuery} for details.</li>
 * <li><b>Full-text query.</b> Uses full-text search engine based on Apache Lucene engine.
 * See {@link TextQuery} for details.</li>
 * <li><b>Scan query.</b> Provides effective and flexible way to full cache\partition scan.
 * See {@link ScanQuery} for details.</li>
 * <li><b>Continuous query.</b> Provides flexible way to process all existed cache data and all future cache updates as well.
 * See {@link ContinuousQuery} for details.</li>
 * <li><b>Spi query.</b> Allow run queries for pluggable user query engine implementation.
 * See {@link SpiQuery} for details.</li>
 * </ul>
 *
 * @see IgniteCache#query(Query)
 */
public abstract class Query<R> implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default query page size. */
    public static final int DFLT_PAGE_SIZE = 1024;

    /** Page size. */
    private int pageSize = DFLT_PAGE_SIZE;

    /** Local flag. */
    private boolean loc;

    /**
     * Empty constructor.
     */
    Query() {
        // No-op.
    }

    /**
     * Gets optional page size, if {@code 0}, then default is used.
     *
     * @return Optional page size.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Sets optional page size, if {@code 0}, then default is used.
     *
     * @param pageSize Optional page size.
     * @return {@code this} for chaining.
     */
    public Query<R> setPageSize(int pageSize) {
        if (pageSize <= 0)
            throw new IllegalArgumentException("Page size must be above zero.");

        this.pageSize = pageSize;

        return this;
    }

    /**
     * Returns {@code true} if this query should be executed on local node only.
     *
     * @return Local flag.
     */
    public boolean isLocal() {
        return loc;
    }

    /**
     * Sets whether this query should be executed on local node only.
     *
     * @param loc Local flag.
     * @return {@code this} for chaining.
     */
    public Query<R> setLocal(boolean loc) {
        this.loc = loc;

        return this;
    }

    /**
     * Prepares the partitions.
     *
     * @param parts Partitions.
     */
    protected int[] prepare(int[] parts) {
        if (parts == null)
            return null;

        A.notEmpty(parts, "Partitions");

        boolean sorted = true;

        // Try to do validation in one pass, if array is already sorted.
        for (int i = 0; i < parts.length; i++) {
            if (i < parts.length - 1)
                if (parts[i] > parts[i + 1])
                    sorted = false;
                else if (sorted)
                    validateDups(parts[i], parts[i + 1]);

            A.ensure(0 <= parts[i] && parts[i] < CacheConfiguration.MAX_PARTITIONS_COUNT, "Illegal partition");
        }

        // Sort and validate again.
        if (!sorted) {
            Arrays.sort(parts);

            for (int i = 0; i < parts.length; i++) {
                if (i < parts.length - 1)
                    validateDups(parts[i], parts[i + 1]);
            }
        }

        return parts;
    }

    /**
     * @param p1 Part 1.
     * @param p2 Part 2.
     */
    private void validateDups(int p1, int p2) {
        A.ensure(p1 != p2, "Partition duplicates are not allowed: " + p1);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Query.class, this);
    }
}
