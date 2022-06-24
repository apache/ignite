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
import java.util.Objects;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Set item key.
 */
public class GridCacheSetItemKey implements SetItemKey, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteUuid setId;

    /** */
    @GridToStringInclude(sensitive = true)
    private Object item;

    /** */
    @GridToStringExclude
    private HashCodeHolder hashCodeHolder;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheSetItemKey() {
        // No-op.
    }

    /**
     * @param setId Set unique ID.
     * @param item Set item.
     */
    GridCacheSetItemKey(IgniteUuid setId, Object item) {
        this.setId = setId;
        this.item = item;

        if (item instanceof HashCodeHolder) {
            hashCodeHolder = (HashCodeHolder) item;
            this.item = hashCodeHolder.obj;
        }
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
        if (hashCodeHolder != null)
            return hashCodeHolder.hashCode;

        int res = setId == null ? 0 : setId.hashCode();

        res = 31 * res + item.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridCacheSetItemKey that = (GridCacheSetItemKey)o;

        return Objects.equals(this.setId, that.setId) && item.equals(that.item);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeIgniteUuid(out, setId);
        out.writeObject(hashCodeHolder != null ? hashCodeHolder : item);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setId = U.readIgniteUuid(in);
        item = in.readObject();

        if (item instanceof HashCodeHolder) {
            hashCodeHolder = (HashCodeHolder) item;
            item = hashCodeHolder.obj;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSetItemKey.class, this);
    }

    public static class HashCodeHolder implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        private Object obj;

        private int hashCode;

        public HashCodeHolder() {
        }

        public HashCodeHolder(Object obj, int hashCode) {
            this.obj = obj;
            this.hashCode = hashCode;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(obj);
            out.writeInt(hashCode);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            obj = in.readObject();
            hashCode = in.readInt();
        }
    }
}
