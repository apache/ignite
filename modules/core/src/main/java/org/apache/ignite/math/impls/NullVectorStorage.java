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

import org.apache.ignite.math.UnsupportedOperationException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Null object for VectorStorage.
 */
final class NullVectorStorage implements VectorStorage {
    /** Unsupported operation. */
    private static final String UNSUPPORTED_OPERATION = "Unsupported operation.";

    /** {@inheritDoc} */ @Override
    public int size() {
        return 0;
    }

    public NullVectorStorage() {
        
    }

    /** {@inheritDoc} */ @Override
    public double get(int i) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */ @Override
    public void set(int i, double v) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */ @Override
    public boolean isArrayBased() {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */ @Override
    public double[] data() {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */ @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */ @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    /** {@inheritDoc} */ @Override
    public boolean equals(Object obj) {
        return this.getClass() == obj.getClass();
    }
}
