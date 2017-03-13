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
 * Identifying TypeDescriptor by space and value class.
 */
public class QueryTypeIdKey {
    /** */
    private final String space;

    /** Value type. */
    private final Class<?> valType;

    /** Value type ID. */
    private final int valTypeId;

    /**
     * Constructor.
     *
     * @param space Space name.
     * @param valType Value type.
     */
    public  QueryTypeIdKey(String space, Class<?> valType) {
        assert valType != null;

        this.space = space;
        this.valType = valType;

        valTypeId = 0;
    }

    /**
     * Constructor.
     *
     * @param space Space name.
     * @param valTypeId Value type ID.
     */
    public QueryTypeIdKey(String space, int valTypeId) {
        this.space = space;
        this.valTypeId = valTypeId;

        valType = null;
    }

    /**
     * @return Space.
     */
    public String space() {
        return space;
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
            (space != null ? space.equals(typeId.space) : typeId.space == null);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (space != null ? space.hashCode() : 0) + (valType != null ? valType.hashCode() : valTypeId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryTypeIdKey.class, this);
    }
}
