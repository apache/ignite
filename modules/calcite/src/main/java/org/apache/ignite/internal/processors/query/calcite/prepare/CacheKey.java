/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
    private final String schemaName;
    private final String query;
    private final Object contextKey;


    public CacheKey(String schemaName, String query, Object contextKey) {
        this.schemaName = schemaName;
        this.query = query;
        this.contextKey = contextKey;
    }

    public CacheKey(String schemaName, String query) {
        this(schemaName, query, null);
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CacheKey that = (CacheKey) o;

        if (!schemaName.equals(that.schemaName))
            return false;
        if (!query.equals(that.query))
            return false;
        return Objects.equals(contextKey, that.contextKey);
    }

    @Override public int hashCode() {
        int result = schemaName.hashCode();
        result = 31 * result + query.hashCode();
        result = 31 * result + (contextKey != null ? contextKey.hashCode() : 0);
        return result;
    }
}
