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

package org.apache.ignite.math.impls.storage;

import org.apache.ignite.math.*;
import org.apache.ignite.math.UnsupportedOperationException;
import java.io.*;

/**
 * No-op matrix storage.
 */
public class MatrixNullStorage implements MatrixStorage {
    /** Unsupported operation. */
    private static final String UNSUPPORTED_OPERATION = "Unsupported matrix operation.";

    @Override
    public double get(int x, int y) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public void set(int x, int y, double v) {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public int columnSize() {
        return 0;
    }

    @Override
    public int rowSize() {
        return 0;
    }

    @Override
    public boolean isArrayBased() {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public double[][] data() {
        throw new UnsupportedOperationException(UNSUPPORTED_OPERATION);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // No-op.
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op.
    }

    /** {@inheritDoc} */ @Override
    public boolean equals(Object obj) {
        return this.getClass() == obj.getClass();
    }
}
