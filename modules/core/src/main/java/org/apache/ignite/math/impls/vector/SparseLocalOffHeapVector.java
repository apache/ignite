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

import java.util.Map;
import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.UnsupportedOperationException;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.matrix.SparseLocalOffHeapMatrix;
import org.apache.ignite.math.impls.storage.vector.SparseOffHeapVectorStorage;

/**
 * TODO: add description.
 */
public class SparseLocalOffHeapVector extends AbstractVector {

    public SparseLocalOffHeapVector(Map<String, Object> args) {
        super(args == null || !args.containsKey("size") ? 0 : (int) args.get("size"));
        assert args != null;

        if (args.containsKey("size"))
            setStorage(new SparseOffHeapVectorStorage((int) args.get("size")));
        else if (args.containsKey("arr") && args.containsKey("copy")) {
            double[] arr = (double[])args.get("arr");

            setStorage(new SparseOffHeapVectorStorage(arr.length));

            assign(arr);
        }
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    public SparseLocalOffHeapVector(int crd) {
        super(crd);
        setStorage(new SparseOffHeapVectorStorage(crd));
    }

    @Override public Vector like(int crd) {
        return new SparseLocalOffHeapVector(crd);
    }

    @Override public Matrix likeMatrix(int rows, int cols) {
        return new SparseLocalOffHeapMatrix(rows, cols);
    }
}
