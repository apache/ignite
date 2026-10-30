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

package org.apache.ignite.internal.thread.context;

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.thread.context.OperationContextSnapshotMessage.MAX_ATTRS_CNT;

/**
 * Represents a key that is used to consistently identify {@link OperationContext} attributes across
 * all nodes in the cluster.
 *
 * @see DistributedOperationContext
 */
public final class DistributedAttributeKey {
    /** Key reserved for the distributed Security Context attribute. */
    public static final DistributedAttributeKey SECURITY = new DistributedAttributeKey(0);

    /** */
    @GridToStringInclude
    private final byte id;

    /** */
    DistributedAttributeKey(int id) {
        assert 0 <= id && id < MAX_ATTRS_CNT : "Invalid distributed attribute id [id=" + id + ']';

        this.id = (byte)id;
    }

    /** @return Cluster-wide id of the attribute. */
    public byte id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof DistributedAttributeKey))
            return false;

        return id == ((DistributedAttributeKey)o).id;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DistributedAttributeKey.class, this);
    }
}
