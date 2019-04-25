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

package org.apache.ignite.internal.processors.datastructures;

import org.apache.ignite.internal.processors.cache.GridCacheInternal;

import java.io.Serializable;

/**
 * Key used to store in utility cache information about created data structures.
 */
public class DataStructureInfoKey implements GridCacheInternal, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Data structure name. */
    private String name;

    /**
     * @param name Data structure name.
     */
    public DataStructureInfoKey(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        DataStructureInfoKey key2 = (DataStructureInfoKey)o;

        return name != null ? name.equals(key2.name) : key2.name == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}
