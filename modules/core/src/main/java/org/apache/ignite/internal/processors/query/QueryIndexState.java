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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;

/**
 * Dynamic index state
 */
public class QueryIndexState implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index name. */
    private final String idxName;

    /** Index. */
    @GridToStringInclude
    private final QueryIndex idx;

    /**
     * Constructor.
     *
     * @param idxName Index name.
     * @param idx Index descriptor.
     */
    public QueryIndexState(String idxName, @Nullable QueryIndex idx) {
        this.idxName = idxName;
        this.idx = idx;
    }

    /**
     * @return Index name.
     */
    public String indexName() {
        return idxName;
    }

    /**
     * @return Index.
     */
    @Nullable public QueryIndex index() {
        return idx;
    }

    /**
     * @return {@code True} if index is removed.
     */
    public boolean removed() {
        return idx == null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryIndexState.class, this);
    }
}
