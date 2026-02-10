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

package org.apache.ignite.internal.management.cache;

import java.util.List;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for {@link org.apache.ignite.cache.QueryIndex}.
 */
public class QueryIndex extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Name of index. */
    @Order(0)
    String name;

    /** Type of index. */
    @Order(1)
    QueryIndexType type;

    /** Fields to create group indexes for. */
    @Order(2)
    List<QueryIndexField> fields;

    /**
     * Create data transfer object for given cache type metadata.
     */
    public QueryIndex() {
        // No-op.
    }

    /**
     * Create data transfer object for given cache type metadata.
     *
     * @param idx Actual cache query entity index.
     */
    public QueryIndex(org.apache.ignite.cache.QueryIndex idx) {
        assert idx != null;

        name = idx.getName();
        type = idx.getIndexType();
        fields = QueryIndexField.list(idx);
    }

    /**
     * @return Name of index.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Type of index.
     */
    public QueryIndexType getType() {
        return type;
    }

    /**
     * @return Fields to create group indexes for.
     */
    public List<QueryIndexField> getFields() {
        return fields;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryIndex.class, this);
    }
}
