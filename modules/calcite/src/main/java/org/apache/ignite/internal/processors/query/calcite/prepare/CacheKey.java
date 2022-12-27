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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.Arrays;
import java.util.Objects;
import org.apache.ignite.internal.util.typedef.X;

/**
 *
 */
public class CacheKey {
    /** */
    private final String schemaName;

    /** */
    private final String query;

    /** */
    private final Object contextKey;

    /** */
    private final Class<?>[] paramTypes;

    /**
     * @param schemaName Schema name.
     * @param query Query string.
     * @param contextKey Optional context key to differ queries with and without/different flags, having an impact
     *                   on result plan (like LOCAL flag)
     * @param params Dynamic parameters.
     */
    public CacheKey(String schemaName, String query, Object contextKey, Object[] params) {
        this.schemaName = schemaName;
        this.query = query;
        this.contextKey = contextKey;
        paramTypes = params.length == 0 ? null :
            Arrays.stream(params).map(p -> (p != null) ? p.getClass() : Void.class).toArray(Class[]::new);;
    }

    /**
     * @param schemaName Schema name.
     * @param query Query string.
     */
    public CacheKey(String schemaName, String query) {
        this(schemaName, query, null, X.EMPTY_OBJECT_ARRAY);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CacheKey cacheKey = (CacheKey)o;

        if (!schemaName.equals(cacheKey.schemaName))
            return false;

        if (!query.equals(cacheKey.query))
            return false;

        if (!Objects.equals(contextKey, cacheKey.contextKey))
            return false;

        return Arrays.deepEquals(paramTypes, cacheKey.paramTypes);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = schemaName.hashCode();
        result = 31 * result + query.hashCode();
        result = 31 * result + (contextKey != null ? contextKey.hashCode() : 0);
        result = 31 * result + Arrays.deepHashCode(paramTypes);
        return result;
    }
}
