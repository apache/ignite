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
import org.apache.ignite.math.impls.matrix.SparseLocalOnHeapMatrix;
import org.apache.ignite.math.impls.storage.vector.SequentialAccessSparseVectorStorage;
import org.apache.ignite.math.impls.storage.vector.RandomAccessSparseVectorStorage;

import java.util.*;

/**
 * TODO: add description.
 */
public class SparseLocalOnHeapVector extends AbstractVector {
    /**
     * 0 - sequential access mode, 1 - random access mode.
     *
     * Random access mode is default mode.
     */
    private int mode = 1;

    /** */
    public SparseLocalOnHeapVector(){
        // No-op.
    }

    /**
     * Create a new sparse local onheap vector. This vector could use {@link SequentialAccessSparseVectorStorage}
     * or {@link RandomAccessSparseVectorStorage}.
     *
     * @param cardinality Vector cardinality.
     * @param mode Access mode.
     */
    public SparseLocalOnHeapVector(int cardinality, int mode){
        super(cardinality);

        setStorage(new SequentialAccessSparseVectorStorage());
    }

    /** */
    public SparseLocalOnHeapVector(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("accessMode"))
            mode = (int) args.get("accessMode");

        if (args.containsKey("size")) {
            setStorage(selectStorage(mode, (int) args.get("size")));
        }
        else if (args.containsKey("arr") && args.containsKey("copy")) { // TODO: add copy support
            double[] arr = (double[])args.get("arr");

            setStorage(selectStorage(mode, arr.length));
            assign(arr);
        }
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    public SparseLocalOnHeapVector(int size) {
        this(size, 1);
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        return new SparseLocalOnHeapVector(crd, mode);
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return new SparseLocalOnHeapMatrix(rows, cols);
    }

    /** */
    private VectorStorage selectStorage(int newMode, int size){
        switch (newMode){
            case 0:
                return new SequentialAccessSparseVectorStorage();
            case 1:
                return new RandomAccessSparseVectorStorage(size);
            default:
                throw new java.lang.UnsupportedOperationException("This access mode is unsupported.");
        }
    }

    @Override
    public int size() {
        return super.size();
    }
}
