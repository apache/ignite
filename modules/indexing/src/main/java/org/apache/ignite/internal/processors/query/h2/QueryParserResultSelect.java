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

import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.h2.command.Prepared;

import java.util.List;

/**
 * Parsing result for SELECT.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class QueryParserResultSelect {
    /** Two-step query, or {@code} null if this result is for local query. */
    private final GridCacheTwoStepQuery twoStepQry;

    /** Metadata for two-step query, or {@code} null if this result is for local query. */
    private final List<GridQueryFieldMetadata> twoStepQryMeta;

    /** Prepared statement for local query. */
    private final Prepared locPrepared;

    public QueryParserResultSelect(
        GridCacheTwoStepQuery twoStepQry,
        List<GridQueryFieldMetadata> twoStepQryMeta,
        Prepared locPrepared
    ) {
        this.twoStepQry = twoStepQry;
        this.twoStepQryMeta = twoStepQryMeta;
        this.locPrepared = locPrepared;
    }

    /**
     * @return Two-step query, or {@code} null if this result is for local query.
     */
    public GridCacheTwoStepQuery twoStepQuery() {
        return twoStepQry;
    }

    /**
     * @return Two-step query metadata.
     */
    public List<GridQueryFieldMetadata> twoStepQueryMeta() {
        return twoStepQryMeta;
    }

    /**
     * @return Prepared statement for local query.
     */
    public Prepared localPrepared() {
        return locPrepared;
    }
}
