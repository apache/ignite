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
import org.apache.ignite.math.impls.storage.vector.VectorOffHeapStorage;

import java.util.*;
import java.util.stream.*;

/**
 * TODO: add description.
 */
public class DenseLocalOffHeapVector extends AbstractVector {
    /** */
    private void makeOffheapStorage(int size){
        setStorage(new VectorOffHeapStorage(size));
    }

    /**
     * @param args Parameters for new Vector.
     */
    public DenseLocalOffHeapVector(Map<String, Object> args) {
        super(args == null || !args.containsKey("size") ? 0 : (int) args.get("size"));
        assert args != null;

        if (args.containsKey("size"))
            makeOffheapStorage((int) args.get("size"));
        else if (args.containsKey("arr") && args.containsKey("copy")) {
            double[] arr = (double[])args.get("arr");

            makeOffheapStorage(arr.length);

            assign(arr);
        }
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /**
     *
     * @param arr Array to copy to offheap storage.
     */
    public DenseLocalOffHeapVector(double[] arr){
        super(arr == null ? 0 : arr.length);
        if (arr == null) {
            setStorage(null);

            return;
        }

        makeOffheapStorage(arr.length);

        assign(arr);
    }

    /**
     * @param size Vector cardinality.
     */
    public DenseLocalOffHeapVector(int size){
        super(size);
        makeOffheapStorage(size);
    }

    /** {@inheritDoc} */
    @Override public Vector assign(Vector vec) {
        checkCardinality(vec);

        IntStream.range(0, size()).parallel().forEach(idx -> set(idx, vec.get(idx)));

        return this;
    }

    /** {@inheritDoc */
    @Override public Vector times(double x) {
        if (x == 0.0)
            return like(size()).assign(0);
        else
            return super.times(x);
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        return new DenseLocalOffHeapVector(crd);
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return new DenseLocalOffHeapMatrix(rows, cols);
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        getStorage().destroy();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return o != null && getClass().equals(o.getClass()) && (getStorage().equals(((Vector)o).getStorage()));
    }
}
