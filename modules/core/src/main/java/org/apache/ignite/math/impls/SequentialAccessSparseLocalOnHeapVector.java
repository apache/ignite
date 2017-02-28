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
import org.apache.ignite.math.impls.storage.vector.SequentialAccessSparseVectorStorage;
import java.util.*;

/**
 * TODO: add description.
 */
public class SequentialAccessSparseLocalOnHeapVector extends AbstractVector  {
    /** */ private int cardinality;

    /** */
    public SequentialAccessSparseLocalOnHeapVector(){
        // No-op.
    }

    /** */
    public SequentialAccessSparseLocalOnHeapVector(int cardinality){
        super(cardinality);

        this.cardinality = cardinality;

        setStorage(new SequentialAccessSparseVectorStorage());
    }

    /** */
    public SequentialAccessSparseLocalOnHeapVector(Vector vector) {
        super(vector);

        this.cardinality = vector == null ? 0 : vector.size();
    }

    /** */
    public SequentialAccessSparseLocalOnHeapVector(Map<String, Object> args) {
        assert args != null;

        if (args.containsKey("size")) {
            cardinality = (int) args.get("size");

            setStorage(new SequentialAccessSparseVectorStorage(), cardinality);
        }
        else if (args.containsKey("arr") && args.containsKey("copy")) {
            double[] arr = (double[])args.get("arr");

            cardinality = arr == null ? 0 : arr.length;

            setStorage(new SequentialAccessSparseVectorStorage(arr, false));
        }
        else
            throw new UnsupportedOperationException("Invalid constructor argument(s).");
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return cardinality;
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        return new SequentialAccessSparseLocalOnHeapVector(crd);
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return null;
    }
}
