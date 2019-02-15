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

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Identifying TypeDescriptor by cache name and value class.
 */
public class QueryTypeIdKey {
    /** */
    private final String cacheName;

    /** Value type. */
    private final Class<?> valType;

    /** Value type ID. */
    private final int valTypeId;

    /**
     * Constructor.
     *
     * @param cacheName Cache name.
     * @param valType Value type.
     */
    public  QueryTypeIdKey(String cacheName, Class<?> valType) {
        assert valType != null;

        this.cacheName = cacheName;
        this.valType = valType;

        valTypeId = 0;
    }

    /**
     * Constructor.
     *
     * @param cacheName Cache name.
     * @param valTypeId Value type ID.
     */
    public QueryTypeIdKey(String cacheName, int valTypeId) {
        this.cacheName = cacheName;
        this.valTypeId = valTypeId;

        valType = null;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryTypeIdKey typeId = (QueryTypeIdKey)o;

        return (valTypeId == typeId.valTypeId) &&
            (valType != null ? valType == typeId.valType : typeId.valType == null) &&
            (cacheName != null ? cacheName.equals(typeId.cacheName) : typeId.cacheName == null);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (cacheName != null ? cacheName.hashCode() : 0) + (valType != null ? valType.hashCode() : valTypeId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryTypeIdKey.class, this);
    }
}
