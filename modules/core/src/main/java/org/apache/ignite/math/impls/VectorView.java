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

package org.apache.ignite.math.impls;

import org.apache.ignite.math.*;
import org.apache.ignite.math.impls.storage.*;
import java.io.*;

/**
 * TODO: add description.
 */
public class VectorView extends AbstractVector {
    /** Parent. */
    private Vector parent;

    /** View offset. */
    private int off;

    /** View length. */
    private int len;

    /**
     * Constructor for {@link Externalizable} interface.
     */
    public VectorView(){
        // No-op.
    }

    /**
     *
     * @param parent
     * @param off
     * @param len
     */
    public VectorView(Vector parent, int off, int len) {
        super(new VectorDelegateStorage(parent.getStorage(), off, len), len);

        this.parent = parent;
        this.off = off;
        this.len = len;
    }

    /** {@inheritDoc} */
    @Override public Vector copy() {
        return new VectorView(parent, off, len);
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        return parent.like(crd);
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return parent.likeMatrix(rows, cols);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(parent);
        out.writeInt(off);
        out.writeInt(len);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        parent = (Vector)in.readObject();
        off = in.readInt();
        len = in.readInt();

        setStorage(new VectorDelegateStorage(parent.getStorage(), off, len));
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return this == o ||
            ((o!=null)
                && o.getClass() == getClass()
                && (getStorage().equals(((VectorView)o).getStorage()))
                && len == ((VectorView)o).len
                && off == ((VectorView)o).off);
    }
}
