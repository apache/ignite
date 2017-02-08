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
 * Basic implementation for vector.
 * <p>
 * This is a trivial implementation for vector assuming dense logic, local on-heap JVM storage
 * based on <code>double[]</code> array. It is only suitable for data sets where
 * local, non-distributed execution is satisfactory and on-heap JVM storage is enough
 * to keep the entire data set.
 */
public class DenseLocalOnHeapVector extends AbstractVector {
    /** */
    private final int DFLT_SIZE = 100;

    /**
     * @param size Vector cardinality.
     */
    private VectorStorage mkStorage(int size) {
        return new VectorArrayStorage(size);
    }

    /**
     * @param arr
     * @param shallowCp
     */
    private VectorStorage mkStorage(double[] arr, boolean shallowCp) {
        return new VectorArrayStorage(shallowCp ? arr : arr.clone());
    }

    /**
     * @param args
     */
    public DenseLocalOnHeapVector(Map<String, Object> args) {
        if (args == null)
            setStorage(mkStorage(DFLT_SIZE));
        else if (args.containsKey("size"))
            setStorage(mkStorage((int) args.get("size")));
        else if (args.containsKey("arr") && args.containsKey("shallowCopy"))
            setStorage(mkStorage((double[])args.get("arr"), (boolean)args.get("shallowCopy")));
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /** */
    public DenseLocalOnHeapVector() {
        setStorage(mkStorage(DFLT_SIZE));
    }

    /**
     * @param size Vector cardinality.
     */
    public DenseLocalOnHeapVector(int size) {
        setStorage(mkStorage(size));
    }

    /**
     * @param arr
     * @param shallowCp
     */
    public DenseLocalOnHeapVector(double[] arr, boolean shallowCp) {
        setStorage(mkStorage(arr, shallowCp));
    }

    /**
     * @param arr
     */
    public DenseLocalOnHeapVector(double[] arr) {
        this(arr, false);
    }

    /**
     *
     * @param orig
     */
    private DenseLocalOnHeapVector(DenseLocalOnHeapVector orig) {
        setStorage(mkStorage(orig.size()));

        assign(orig);
    }

    /** {@inheritDoc */
    @Override public Vector copy() {
        return new DenseLocalOnHeapVector(this);
    }

    /** {@inheritDoc */
    @Override public Vector like(int crd) {
        return new DenseLocalOnHeapVector(crd);
    }
}
