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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.util.typedef.F;

/**
 * Key for types lookup.
 */
public class H2TypeKey {
    /** Cache name. */
    private final String cacheName;

    /** Type name. */
    private final String typeName;

    /**
     * Constructor.
     *
     * @param cacheName Cache name.
     * @param typeName Type name.
     */
    H2TypeKey(String cacheName, String typeName) {
        this.cacheName = cacheName;
        this.typeName = typeName;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        H2TypeKey other = (H2TypeKey)o;

        return F.eq(typeName, other.typeName) && F.eq(cacheName, other.cacheName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = cacheName.hashCode();

        res = 31 * res + typeName.hashCode();

        return res;
    }
}
