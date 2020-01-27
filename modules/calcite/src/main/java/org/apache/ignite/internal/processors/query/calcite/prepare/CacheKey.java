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

import java.util.Objects;

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

    /**
     * @param schemaName Schema name.
     * @param query Query string.
     * @param contextKey Optional context key to differ queries with and without/different flags, having an impact
     *                   on result plan (like LOCAL flag)
     */
    public CacheKey(String schemaName, String query, Object contextKey) {
        this.schemaName = schemaName;
        this.query = query;
        this.contextKey = contextKey;
    }

    /**
     * @param schemaName Schema name.
     * @param query Query string.
     */
    public CacheKey(String schemaName, String query) {
        this(schemaName, query, null);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CacheKey cacheKey = (CacheKey) o;

        if (!schemaName.equals(cacheKey.schemaName))
            return false;
        if (!query.equals(cacheKey.query))
            return false;
        return Objects.equals(contextKey, cacheKey.contextKey);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = schemaName.hashCode();
        result = 31 * result + query.hashCode();
        result = 31 * result + (contextKey != null ? contextKey.hashCode() : 0);
        return result;
    }
}
