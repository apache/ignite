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
public class CollocatedSetItemKey implements SetItemKey {
    /** */
    private IgniteUuid setId;

    /** */
    @GridToStringInclude(sensitive = true)
    private Object item;

    /** */
    @AffinityKeyMapped
    private int setNameHash;

    /**
     * @param setName Set name.
     * @param setId Set unique ID.
     * @param item Set item.
     */
    public CollocatedSetItemKey(String setName, IgniteUuid setId, Object item) {
        this.setNameHash = setName.hashCode();
        this.setId = setId;
        this.item = item;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid setId() {
        return setId;
    }

    /** {@inheritDoc} */
    @Override public Object item() {
        return item;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = setId.hashCode();

        res = 31 * res + item.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        CollocatedSetItemKey that = (CollocatedSetItemKey)o;

        return setId.equals(that.setId) && item.equals(that.item);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CollocatedSetItemKey.class, this);
    }
}
