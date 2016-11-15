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

package org.apache.ignite.cache.spring;

import java.io.Serializable;

/**
 * Complex key.
 */
public class GridSpringCacheTestKey implements Serializable {
    /** */
    private final Integer p1;

    /** */
    private final String p2;

    /**
     * @param p1 Parameter 1.
     * @param p2 Parameter 2.
     */
    public GridSpringCacheTestKey(Integer p1, String p2) {
        assert p1 != null;
        assert p2 != null;

        this.p1 = p1;
        this.p2 = p2;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridSpringCacheTestKey key = (GridSpringCacheTestKey)o;

        return p1.equals(key.p1) && p2.equals(key.p2);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * p1 + p2.hashCode();
    }
}
