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

package org.apache.ignite.cache.store.jdbc;

import java.util.Collection;

/**
 * Default implementation of {@link JdbcTypeHasher}.
 *
 * This implementation ignores type and field names.
 */
public class JdbcTypeDefaultHasher implements JdbcTypeHasher {
    /** */
    private static final long serialVersionUID = 0L;

    /** Singleton instance to use. */
    public static final JdbcTypeHasher INSTANCE = new JdbcTypeDefaultHasher();

    /** {@inheritDoc} */
    @Override public int hashCode(Collection<?> values) {
        int hash = 0;

        for (Object val : values)
            hash = 31 * hash + (val != null ? val.hashCode() : 0);

        return hash;
    }
}
