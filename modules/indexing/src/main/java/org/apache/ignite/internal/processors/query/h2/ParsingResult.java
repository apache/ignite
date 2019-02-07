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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.sql.command.SqlCommand;
import org.h2.command.Prepared;

/**
 * Result of parsing and splitting SQL from {@link SqlFieldsQuery}.
 */
final class ParsingResult {
    /** H2 command. */
    private final Prepared prepared;

    /** New fields query that may be executed right away. */
    private final SqlFieldsQuery newQry;

    /** Remaining SQL statements. */
    private final String remainingSql;

    /** Two-step query, or {@code} null if this result is for local query. */
    private final GridCacheTwoStepQuery twoStepQry;

    /**
     * Two-step query key to cache {@link #twoStepQry}, or {@code null} if there's no need to worry about two-step
     * caching.
     */
    private final H2TwoStepCachedQueryKey twoStepQryKey;

    /** Metadata for two-step query, or {@code} null if this result is for local query. */
    private final List<GridQueryFieldMetadata> meta;

    /** Parsed native command/ */
    private final SqlCommand newNativeCmd;

    /**
     * Simple constructor.
     */
    private ParsingResult(
        Prepared prepared,
        SqlFieldsQuery newQry,
        String remainingSql,
        GridCacheTwoStepQuery twoStepQry,
        H2TwoStepCachedQueryKey twoStepQryKey,
        List<GridQueryFieldMetadata> meta,
        SqlCommand newNativeCmd) {
        this.prepared = prepared;
        this.newQry = newQry;
        this.remainingSql = remainingSql;
        this.twoStepQry = twoStepQry;
        this.twoStepQryKey = twoStepQryKey;
        this.meta = meta;
        this.newNativeCmd = newNativeCmd;
    }

    /**
     * Construct result in case of h2 parsing and two step query.
     */
    public ParsingResult(
        Prepared prepared,
        SqlFieldsQuery newQry,
        String remainingSql,
        GridCacheTwoStepQuery twoStepQry,
        H2TwoStepCachedQueryKey twoStepQryKey,
        List<GridQueryFieldMetadata> meta) {
        this(prepared, newQry, remainingSql, twoStepQry, twoStepQryKey, meta, null);
    }

    /**
     * Construct parsing result in case of native parsing.
     *
     * @param newQry leading sql statement of the original multi-statement query.
     * @param newNativeCmd parsed sql command. Represents newQry.
     * @param remainingSql the rest of the original query.
     */
    public ParsingResult(SqlFieldsQuery newQry, SqlCommand newNativeCmd, String remainingSql) {
        this(null, newQry, remainingSql, null, null, null, newNativeCmd);
    }

    /**
     * Result in case we use h2 but don't have two step query.
     *
     * @param prepared h2's parsed prepared statement. The leading statement of the original multi-statement query.
     * @param newQry SqlFields query that prepare represents.
     * @param remainingSql the rest of the original query.
     */
    public ParsingResult(Prepared prepared, SqlFieldsQuery newQry, String remainingSql) {
        this(prepared, newQry, remainingSql, null, null, null, null);
    }

    /**
     * @return Metadata for two-step query, or {@code} null if this result is for local query.
     */
    List<GridQueryFieldMetadata> meta() {
        return meta;
    }

    /**
     * @return New fields query that may be executed right away.
     */
    SqlFieldsQuery newQuery() {
        return newQry;
    }

    /**
     * Sql command produced by native sql parser.
     */
    public SqlCommand newNativeCommand() {
        return newNativeCmd;
    }

    /**
     * @return H2 command.
     */
    Prepared prepared() {
        return prepared;
    }

    /**
     * @return Remaining SQL statements.
     */
    String remainingSql() {
        return remainingSql;
    }

    /**
     * @return Two-step query, or {@code} null if this result is for local query.
     */
    GridCacheTwoStepQuery twoStepQuery() {
        return twoStepQry;
    }

    /**
     * @return Two-step query key to cache {@link #twoStepQry}, or {@code null} if there's no need to worry about
     * two-step caching.
     */
    H2TwoStepCachedQueryKey twoStepQueryKey() {
        return twoStepQryKey;
    }
}
