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
import org.apache.ignite.math.impls.storage.matrix.MatrixArrayStorage;
import org.apache.ignite.math.impls.storage.matrix.MatrixOffHeapStorage;

import java.util.*;

/**
 * TODO add description
 */
public class DenseLocalOffHeapMatrix extends AbstractMatrix {
    /**
     *
     * @param data
     */
    public DenseLocalOffHeapMatrix(double[][] data){
        setStorage(new MatrixOffHeapStorage(data));
    }

    /**
     *
     * @param rows
     * @param cols
     */
    public DenseLocalOffHeapMatrix(int rows, int cols){
        setStorage(new MatrixOffHeapStorage(rows, cols));
    }

    /**
     *
     * @param args
     */
    public DenseLocalOffHeapMatrix(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("rows") && args.containsKey("cols"))
            setStorage(new MatrixArrayStorage((int)args.get("rows"), (int)args.get("cols")));
        else if (args.containsKey("arr"))
            setStorage(new MatrixArrayStorage((double[][])args.get("arr")));
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /** {@inheritDoc} */
    @Override
    public Matrix copy() {
        DenseLocalOffHeapMatrix copy = new DenseLocalOffHeapMatrix(getStorage().rowSize(), getStorage().rowSize());

        copy.assign(this);

        return copy;
    }

    /** {@inheritDoc} */
    @Override
    public Matrix like(int rows, int cols) {
        return new DenseLocalOffHeapMatrix(rows, cols);
    }

    /** {@inheritDoc} */
    @Override
    public Vector likeVector(int crd) {
        return new DenseLocalOffHeapVector(crd);
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        getStorage().destroy();
    }
}
