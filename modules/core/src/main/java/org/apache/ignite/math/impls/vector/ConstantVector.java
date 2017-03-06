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

package org.apache.ignite.math.impls.vector;

import org.apache.ignite.math.*;
import org.apache.ignite.math.UnsupportedOperationException;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.storage.vector.ConstantVectorStorage;

import java.util.*;
import org.apache.ignite.math.impls.storage.vector.RandomVectorStorage;

/**
 * Constant value, read-only vector.
 */
public class ConstantVector extends AbstractReadonlyVector {
    /**
     *
     */
    public ConstantVector() {
        // No-op.
    }

    /**
     *
     * @param size
     * @param val
     */
    public ConstantVector(int size, double val) {
        super(true, new ConstantVectorStorage(size, val));
    }

    /**
     *
     * @return
     */
    private ConstantVectorStorage storage() {
        return (ConstantVectorStorage)getStorage();
    }

    /**
     * @param args
     */
    public ConstantVector(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("size") && args.containsKey("value"))
            setStorage(new ConstantVectorStorage((int)args.get("size"), (double)args.get("value")));
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /** {@inheritDoc} */
    @Override public Vector copy() {
        ConstantVectorStorage sto = storage();

        return new ConstantVector(sto.size(), sto.constant());
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        return new ConstantVector(crd, storage().constant());
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return null; // TODO
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ConstantVector that = (ConstantVector) o;
        VectorStorage thisStorage = getStorage();

        return (thisStorage != null ? thisStorage.equals(that.getStorage()) : that.getStorage() == null);
    }
}
