/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.math.primitives.vector.storage;

import java.io.Serializable;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorStorage;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;

/**
 * Tests for DenseVectorStorage.
 */
public class DenseVectorStorageTest extends AbstractStorageTest {

    /** {@inheritDoc} */
    @Override protected boolean isNumericVector(VectorStorage storage) {
        try {
            double[] arr = ((DenseVectorStorage)storage).getData();
            return storage.isNumeric() && (arr != null || !isRaw(storage));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean isRaw(VectorStorage storage) {
        try {
            Serializable[] arr = ((DenseVectorStorage) storage).getRawData();
            return arr != null;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected VectorStorage createStorage(int size) {
        return new DenseVectorStorage(size);
    }

    /** {@inheritDoc} */
    @Override protected Vector createVector(int size) {
        return new DenseVector(size);
    }
}
