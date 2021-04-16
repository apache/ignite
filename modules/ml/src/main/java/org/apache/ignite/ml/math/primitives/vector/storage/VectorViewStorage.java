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

package org.apache.ignite.ml.math.primitives.vector.storage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import org.apache.ignite.ml.math.primitives.vector.VectorStorage;

/**
 * {@link VectorStorage} implementation that delegates to parent matrix.
 */
public class VectorViewStorage implements VectorStorage {
    /** Parent vector storage. */
    private VectorStorage sto;

    /** Offset in the parent vector. */
    private int off;

    /** Size of the vector. */
    private int len;

    /**
     *
     */
    public VectorViewStorage() {
        // No-op.
    }

    /**
     * @param sto Vector storage to delegate to.
     * @param off Offset in the parent vector.
     * @param len Size of the vector.
     */
    public VectorViewStorage(VectorStorage sto, int off, int len) {
        assert sto != null;
        assert off >= 0;
        assert len > 0;

        this.sto = sto;
        this.off = off;
        this.len = len;
    }

    /**
     * @return Backing vector storage.
     */
    public VectorStorage delegate() {
        return sto;
    }

    /**
     * @return Offset into the backing vector.
     */
    public int offset() {
        return off;
    }

    /**
     * @return Vector length.
     */
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
    @Override public <T extends Serializable> T getRaw(int i) {
        return sto.getRaw(off + i);
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        sto.set(off + i, v);
    }

    /** {@inheritDoc} */
    @Override public void setRaw(int i, Serializable v) {
        sto.setRaw(off + i, v);
    }

    /** {@inheritDoc} */
    @Override public double[] data() {
        return sto.data();
    }

    /** {@inheritDoc} */
    @Override public Serializable[] rawData() {
        return sto.rawData();
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return sto.isDense();
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return sto.isArrayBased();
    }

    /** {@inheritDoc} */
    @Override public boolean isNumeric() {
        return sto.isNumeric();
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
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        VectorViewStorage that = (VectorViewStorage)o;

        return len == that.len && off == that.off && (sto != null ? sto.equals(that.sto) : that.sto == null);
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
