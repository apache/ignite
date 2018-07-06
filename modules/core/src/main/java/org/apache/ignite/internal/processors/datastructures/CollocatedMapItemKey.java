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
 *
 */
public class CollocatedMapItemKey implements MapItemKey {
    /** */
    private IgniteUuid id;

    /** */
    @GridToStringInclude(sensitive = true)
    private Object item;

    /** */
    @AffinityKeyMapped
    private int setNameHash;

    /**
     * @param name Name.
     * @param id Unique ID.
     * @param item Item.
     */
    public CollocatedMapItemKey(String name, IgniteUuid id, Object item) {
        this.setNameHash = name.hashCode();
        this.id = id;
        this.item = item;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public Object item() {
        return item;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = id.hashCode();

        res = 31 * res + item.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        CollocatedMapItemKey that = (CollocatedMapItemKey)o;

        return id.equals(that.id) && item.equals(that.item);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CollocatedMapItemKey.class, this);
    }
}
