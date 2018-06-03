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

    /** Schema name. */
    private final String schemaName;

    /** Name. */
    private final String name;

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param name Name.
     */
    public QueryIndexKey(String schemaName, String name) {
        this.schemaName = schemaName;
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * (schemaName != null ? schemaName.hashCode() : 0) + (name != null ? name.hashCode() : 0);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        QueryIndexKey other = (QueryIndexKey)o;

        return F.eq(name, other.name) && F.eq(schemaName, other.schemaName);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryIndexKey.class, this);
    }
}
