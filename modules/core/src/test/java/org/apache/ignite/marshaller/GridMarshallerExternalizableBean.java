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

package org.apache.ignite.marshaller;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Externalizable bean for marshaller testing.
 */
class GridMarshallerExternalizableBean implements Externalizable {
    /** */
    private int i;

    /**
     * Required for {@link Externalizable}.
     */
    public GridMarshallerExternalizableBean() {
        // No=op.
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(i);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        i = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        assert o instanceof GridMarshallerExternalizableBean;

        return i == ((GridMarshallerExternalizableBean)o).i;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return i;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridMarshallerExternalizableBean.class, this);
    }
}