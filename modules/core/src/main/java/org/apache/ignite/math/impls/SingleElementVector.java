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
import org.apache.ignite.math.UnsupportedOperationException;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.storage.*;
import java.util.*;

/**
 * Read-write vector holding a single non-zero value at some index.
 */
public class SingleElementVector extends AbstractVector {
    /**
     *
     */
    public SingleElementVector() {
        // No-op
    }

    /**
     *
     * @param size
     * @param idx
     * @param val
     */
    public SingleElementVector(int size, int idx, double val) {
        super(new SingleElementVectorStorage(size, idx, val), 1);
    }

    /**
     * @param args
     */
    public SingleElementVector(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("size") && args.containsKey("index") && args.containsKey("value")) {
            int size = (int)args.get("size");
            int idx = (int)args.get("index");
            double val = (double)args.get("value");

            setStorage(new SingleElementVectorStorage(size, idx, val));
        }
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /**
     *
     * @return
     */
    private SingleElementVectorStorage storage() {
        return (SingleElementVectorStorage)getStorage();
    }

    @Override
    public Element minValue() {
        return makeElement(storage().index());
    }

    @Override
    public Element maxValue() {
        return makeElement(storage().index());
    }

    @Override
    public double sum() {
        return getX(storage().index());
    }

    @Override
    public int nonZeroElements() {
        return 1;
    }

    @Override
    public Vector copy() {
        int idx = storage().index();

        return new SingleElementVector(size(), idx, getX(idx));
    }

    @Override
    public Vector like(int crd) {
        int idx = storage().index();

        return new SingleElementVector(crd, idx, getX(idx));
    }

    @Override
    public Matrix likeMatrix(int rows, int cols) {
        return null; // TODO
    }

    @Override
    public Matrix toMatrix(boolean rowLike) {
        return null; // TODO
    }

    @Override
    public Matrix toMatrixPlusOne(boolean rowLike, double zeroVal) {
        return null; // TODO
    }
}
