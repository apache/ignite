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

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Multimap item key
 */
public class CollocatedMultimapItemKey<K> implements MultimapItemKey<K> {

    /** */
    private IgniteUuid id;

    /** */
    private String multimapName;

    /** */
    @GridToStringInclude
    private K key;

    /** */
    @AffinityKeyMapped
    private int multimapNameHash;

    /**
     * @param id Multimap unique ID.
     * @param multimapName Multimap name.
     * @param key User key.
     */
    public CollocatedMultimapItemKey(IgniteUuid id, String multimapName, K key) {
        this.id = id;
        this.multimapName = multimapName;
        this.key = key;
        this.multimapNameHash = multimapName.hashCode();
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid getId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String getMultimapName() {
        return multimapName;
    }

    /** {@inheritDoc} */
    @Override public K getKey() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CollocatedMultimapItemKey<?> that = (CollocatedMultimapItemKey<?>)o;

        if (multimapNameHash != that.multimapNameHash)
            return false;
        if (!id.equals(that.id))
            return false;
        if (!multimapName.equals(that.multimapName))
            return false;
        return key != null ? key.equals(that.key) : that.key == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + multimapName.hashCode();
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + multimapNameHash;
        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CollocatedMultimapItemKey.class, this);
    }
}
