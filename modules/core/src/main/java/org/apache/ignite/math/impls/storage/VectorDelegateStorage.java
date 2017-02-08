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

package org.apache.ignite.math.impls.storage;

import org.apache.ignite.math.*;
import java.io.*;

/**
 * TODO: add description.
 */
public class VectorDelegateStorage implements VectorStorage {
    /** */
    private VectorStorage sto;

    /** */
    private int off;

    /** */
    private int len;

    /**
     *
     */
    public VectorDelegateStorage() {

    }

    /**
     *
     * @param sto Vector storage to delegate to.
     * @param off
     * @param len
     */
    public VectorDelegateStorage(VectorStorage sto, int off, int len) {
        this.sto = sto;
        this.off = off;
        this.len = len;
    }

    @Override
    public int size() {
        return len;
    }

    @Override
    public boolean isSequentialAccess() {
        return sto.isSequentialAccess();
    }

    @Override
    public boolean isDense() {
        return sto.isDense();
    }

    @Override
    public double getLookupCost() {
        return sto.getLookupCost();
    }

    @Override
    public boolean isAddConstantTime() {
        return sto.isAddConstantTime();
    }

    @Override
    public double get(int i) {
        return sto.get(off + i);
    }

    @Override
    public void set(int i, double v) {
        sto.set(off + i, v);
    }

    @Override
    public boolean isArrayBased() {
        return sto.isArrayBased();
    }

    @Override
    public double[] data() {
        return sto.data();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(sto);
        out.writeInt(off);
        out.writeInt(len);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        sto = (VectorStorage)in.readObject();
        off = in.readInt();
        len = in.readInt();
    }
}
