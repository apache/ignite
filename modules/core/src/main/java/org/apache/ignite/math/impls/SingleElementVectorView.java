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
import org.apache.ignite.math.impls.storage.*;

/**
 * Single value vector view over another vector.
 */
public class SingleElementVectorView extends AbstractVector {
    /**
     *
     */
    public SingleElementVectorView() {
        // No-op.
    }

    /**
     *
     * @param vec
     * @param idx
     */
    public SingleElementVectorView(Vector vec, int idx) {
        super(new SingleElementVectorDelegateStorage(vec, idx), 1);
    }

    /**
     *
     * @return
     */
    private SingleElementVectorDelegateStorage storage() {
        return (SingleElementVectorDelegateStorage)getStorage();
    }

    /**
     *
     * @param sto
     */
    protected SingleElementVectorView(SingleElementVectorDelegateStorage sto) {
        super(sto, 1);
    }

    /** {@inheritDoc} */
    @Override public Element minValue() {
        return makeElement(storage().index());
    }

    /** {@inheritDoc} */
    @Override public Element maxValue() {
        return makeElement(storage().index());
    }

    /** {@inheritDoc} */
    @Override public double sum() {
        return getX(storage().index());
    }

    /** {@inheritDoc} */
    @Override public int nonZeroElements() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public Vector copy() {
        return new SingleElementVectorView(storage());
    }

    /** {@inheritDoc} */
    @Override public Vector like(int crd) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Matrix likeMatrix(int rows, int cols) {
        return null; // TODO
    }
}
