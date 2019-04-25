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
 * Internal key for data structures processor.
 */
public class DataStructuresCacheKey implements GridCacheInternal, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return getClass().getName().hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj == this || (obj instanceof DataStructuresCacheKey);
    }

    @Override public String toString() {
        return "DataStructuresCacheKey []";
    }
}
