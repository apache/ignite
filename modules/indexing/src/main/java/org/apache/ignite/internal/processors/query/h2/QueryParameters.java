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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.NestedTxMode;

/**
 * Query parameters which vary between requests having the same execution plan. Essentially, these are the arguments
 * of original {@link org.apache.ignite.cache.query.SqlFieldsQuery} which are not part of {@link QueryParserCacheKey}.
 */
public class QueryParameters {
    /** Arguments. */
    private final Object[] args;

    /** Partitions. */
    private final int[] parts;

    /** Timeout. */
    private final int timeout;

    /** Lazy flag. */
    private final boolean lazy;

    /** Data page scan enabled flag. */
    private final Boolean dataPageScanEnabled;

    /** Nexted transactional mode. */
    private final NestedTxMode nestedTxMode;

    /**
     * Create parameters from query.
     *
     * @param qry Query.
     * @return Parameters.
     */
    public static QueryParameters fromQuery(SqlFieldsQuery qry) {
        return new QueryParameters(
            qry.getArgs(),
            qry.getPartitions(),
            qry.getTimeout(),
            qry.isLazy(),
            qry.isDataPageScanEnabled(),
            qry instanceof SqlFieldsQueryEx ? ((SqlFieldsQueryEx)qry).getNestedTxMode() : NestedTxMode.DEFAULT
        );
    }

    /**
     * Constructor.
     *
     * @param args Arguments.
     * @param parts Partitions.
     * @param timeout Timeout.
     * @param lazy Lazy flag.
     * @param dataPageScanEnabled Data page scan enabled flag.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    private QueryParameters(
        Object[] args,
        int[] parts,
        int timeout,
        boolean lazy,
        Boolean dataPageScanEnabled,
        NestedTxMode nestedTxMode
    ) {
        this.args = args;
        this.parts = parts;
        this.timeout = timeout;
        this.lazy = lazy;
        this.dataPageScanEnabled = dataPageScanEnabled;
        this.nestedTxMode = nestedTxMode;
    }

    /**
     * @return Arguments.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public Object[] arguments() {
        return args;
    }

    /**
     * @return Partitions.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public int[] partitions() {
        return parts;
    }

    /**
     * @return Timeout.
     */
    public int timeout() {
        return timeout;
    }

    /**
     * @return Lazy flag.
     */
    public boolean lazy() {
        return lazy;
    }

    /**
     * @return Data page scan enabled flag.
     */
    public Boolean dataPageScanEnabled() {
        return dataPageScanEnabled;
    }

    /**
     * @return Nested TX mode.
     */
    public NestedTxMode nestedTxMode() {
        return nestedTxMode;
    }
}
