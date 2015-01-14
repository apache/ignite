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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.GridCacheProjection;

import java.io.*;

/**
 */
public final class GridCacheTransformComputeClosure<V, R> implements IgniteClosure<V, V>, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteClosure<V, IgniteBiTuple<V, R>> transformer;

    /** */
    private R retVal;

    /**
     * Required by {@link Externalizable}.
     */
    public GridCacheTransformComputeClosure() {
        // No-op.
    }

    /**
     * @param transformer Transformer closure.
     */
    public GridCacheTransformComputeClosure(IgniteClosure<V, IgniteBiTuple<V, R>> transformer) {
        this.transformer = transformer;
    }

    /**
     * @return Return value for {@link GridCacheProjection#transformAndCompute(Object, org.apache.ignite.lang.IgniteClosure)}
     */
    public R returnValue() {
        return retVal;
    }

    /** {@inheritDoc} */
    @Override public V apply(V v) {
        IgniteBiTuple<V, R> t = transformer.apply(v);

        retVal = t.get2();

        return t.get1();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(transformer);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        transformer = (IgniteClosure<V, IgniteBiTuple<V, R>>)in.readObject();
    }
}
