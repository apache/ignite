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
import org.apache.ignite.internal.util.typedef.internal.S;

import java.util.List;

/**
 * Cached two-step query.
 */
public class H2TwoStepCachedQuery {
    /** */
    private final List<GridQueryFieldMetadata> meta;

    /** */
    private final GridCacheTwoStepQuery twoStepQry;

    /**
     * @param meta Fields metadata.
     * @param twoStepQry Query.
     */
    public H2TwoStepCachedQuery(List<GridQueryFieldMetadata> meta, GridCacheTwoStepQuery twoStepQry) {
        this.meta = meta;
        this.twoStepQry = twoStepQry;
    }

    /**
     * @return Fields metadata.
     */
    public List<GridQueryFieldMetadata> meta() {
        return meta;
    }

    /**
     * @return Query.
     */
    public GridCacheTwoStepQuery query() {
        return twoStepQry;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2TwoStepCachedQuery.class, this);
    }
}
