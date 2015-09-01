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

package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.processors.cache.GridCacheInternal;

/**
 * Internal key for data structures processor.
 */
public class CacheDataStructuresCacheKey implements GridCacheInternal, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     *
     */
    public CacheDataStructuresCacheKey() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return getClass().getName().hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj == this || (obj instanceof CacheDataStructuresCacheKey);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CacheDataStructuresCacheKey []";
    }
}