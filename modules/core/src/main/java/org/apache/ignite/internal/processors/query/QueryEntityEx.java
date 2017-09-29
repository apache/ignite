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

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Set;

/**
 * Extended query entity with not-null fields support.
 */
public class QueryEntityEx extends QueryEntity {
    /** */
    private static final long serialVersionUID = 0L;

    /** Fields that must have non-null value. */
    private Set<String> notNullFields;

    /**
     * Default constructor.
     */
    public QueryEntityEx() {
        // No-op.
    }

    /**
     * Copying constructor.
     *
     * @param other Instance to copy.
     */
    public QueryEntityEx(QueryEntity other) {
        super(other);

        if (other instanceof QueryEntityEx) {
            QueryEntityEx other0 = (QueryEntityEx)other;

            notNullFields = other0.notNullFields != null ? new HashSet<>(other0.notNullFields) : null;
        }
    }

    /** {@inheritDoc} */
    @Override @Nullable public Set<String> getNotNullFields() {
        return notNullFields;
    }

    /** {@inheritDoc} */
    public QueryEntity setNotNullFields(@Nullable Set<String> notNullFields) {
        this.notNullFields = notNullFields;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryEntityEx entity = (QueryEntityEx)o;

        return super.equals(entity) && F.eq(notNullFields, entity.notNullFields);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = super.hashCode();

        res = 31 * res + (notNullFields != null ? notNullFields.hashCode() : 0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryEntityEx.class, this);
    }
}
