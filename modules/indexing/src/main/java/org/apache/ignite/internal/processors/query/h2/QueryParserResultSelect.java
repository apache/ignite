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
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * Parsing result for SELECT.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class QueryParserResultSelect {
    /** Two-step query, or {@code} null if this result is for local query. */
    private final GridCacheTwoStepQuery twoStepQry;

    /** Whether local split is needed. */
    private final boolean locSplit;

    /** Metadata for two-step query, or {@code} null if this result is for local query. */
    private final List<GridQueryFieldMetadata> meta;

    /**
     * Constructor.
     *
     * @param twoStepQry Distributed query plan.
     * @param locSplit Whether local split is needed.
     * @param meta Fields metadata.
     */
    public QueryParserResultSelect(
        @Nullable GridCacheTwoStepQuery twoStepQry,
        boolean locSplit,
        List<GridQueryFieldMetadata> meta
    ) {
        // Local split can be true only is there is a two-step plan.
        assert twoStepQry == null && !locSplit || twoStepQry != null;

        this.twoStepQry = twoStepQry;
        this.locSplit = locSplit;
        this.meta = meta;
    }

    /**
     * @return Two-step query, or {@code} null if this result is for local query.
     */
    @Nullable public GridCacheTwoStepQuery twoStepQuery() {
        return twoStepQry;
    }

    /**
     * @return {@code True} if local query should be split.
     */
    public boolean localSplit() {
        return locSplit;
    }

    /**
     * @return Two-step query metadata.
     */
    public List<GridQueryFieldMetadata> meta() {
        return meta;
    }

    /**
     * @return Whether split is needed for this query.
     */
    public boolean splitNeeded() {
        return twoStepQry != null;
    }
}
