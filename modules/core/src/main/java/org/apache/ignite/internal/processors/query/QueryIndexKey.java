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

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.Serializable;

/**
 * Index key.
 */
public class QueryIndexKey implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Space. */
    private final String space;

    /** Name. */
    private final String name;

    /**
     * Constructor.
     *
     * @param space Space.
     * @param name Name.
     */
    public QueryIndexKey(String space, String name) {
        this.space = space;
        this.name = name;
    }

    /**
     * @return Space.
     */
    public String space() {
        return space;
    }

    /**
     * @return Name.
     */
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (space != null ? space.hashCode() : 0) + (name != null ? name.hashCode() : 0);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryIndexKey other = (QueryIndexKey)o;

        return F.eq(name, other.name) && F.eq(space, other.space);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryIndexKey.class, this);
    }
}
