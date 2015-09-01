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

package org.apache.ignite.internal.visor.query;

import java.io.Serializable;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Result for cache query tasks.
 */
public class VisorQueryResult implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Rows fetched from query. */
    private final List<Object[]> rows;

    /** Whether query has more rows to fetch. */
    private final boolean hasMore;

    /** Query duration */
    private final long duration;

    /**
     * Create task result with given parameters
     *
     * @param rows Rows fetched from query.
     * @param hasMore Whether query has more rows to fetch.
     * @param duration Query duration.
     */
    public VisorQueryResult(List<Object[]> rows, boolean hasMore, long duration) {
        this.rows = rows;
        this.hasMore = hasMore;
        this.duration = duration;
    }

    /**
     * @return Rows fetched from query.
     */
    public List<Object[]> rows() {
        return rows;
    }

    /**
     * @return Whether query has more rows to fetch.
     */
    public boolean hasMore() {
        return hasMore;
    }

    /**
     * @return Duration of next page fetching.
     */
    public long duration() {
        return duration;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorQueryResult.class, this);
    }
}