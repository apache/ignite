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

package org.apache.ignite.math.impls.storage.vector;

import org.apache.ignite.math.*;
import java.io.*;

/**
 * TODO: add description.
 */
public class VectorDelegateStorage implements VectorStorage {
    private VectorStorage sto;
    private int off, len;

    /**
     *
     */
    public VectorDelegateStorage() {
        // No-op.
    }

    /**
     *
     * @param sto Vector storage to delegate to.
     * @param off
     * @param len
     */
    public VectorDelegateStorage(VectorStorage sto, int off, int len) {
        assert sto != null;
        assert off >= 0;
        assert len > 0;

        this.sto = sto;
        this.off = off;
        this.len = len;
    }

    /** */
    public VectorStorage delegate() {
        return sto;
    }

    /** */
    public int offset() {
        return off;
    }

    /** */
    public int length() {
        return len;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return len;
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        return sto.get(off + i);
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        sto.set(off + i, v);
    }

    /** {@inheritDoc} */
    @Override public double[] data() {
        return sto.data();
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return sto.isSequentialAccess();
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return sto.isDense();
    }

    /** {@inheritDoc} */
    @Override public double getLookupCost() {
        return sto.getLookupCost();
    }

    /** {@inheritDoc} */
    @Override public boolean isAddConstantTime() {
        return sto.isAddConstantTime();
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return sto.isArrayBased();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(sto);
        out.writeInt(off);
        out.writeInt(len);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sto = (VectorStorage)in.readObject();
        off = in.readInt();
        len = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return this == obj ||
            ((obj != null)
                && obj.getClass() == getClass()
                && (sto.equals(((VectorDelegateStorage)obj).sto))
                && len == ((VectorDelegateStorage)obj).len
                && off == ((VectorDelegateStorage)obj).off);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + off;
        res = res * 37 + len;
        res = res * 37 + sto.hashCode();

        return res;
    }
}
