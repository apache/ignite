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
    public QueryTypeIdKey(String cacheName, Class<?> valType) {
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
