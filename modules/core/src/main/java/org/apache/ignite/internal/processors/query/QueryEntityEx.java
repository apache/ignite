/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
    @Override public QueryEntity setNotNullFields(@Nullable Set<String> notNullFields) {
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
