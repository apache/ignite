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

package org.apache.ignite.ml.math.primitives.vector;

import java.io.Externalizable;
import java.io.Serializable;
import org.apache.ignite.ml.math.Destroyable;
import org.apache.ignite.ml.math.StorageOpsMetrics;

/**
 * Data storage support for {@link Vector}.
 */
public interface VectorStorage extends Externalizable, StorageOpsMetrics, Destroyable {
    /**
     *
     *
     */
    public int size();

    /**
     * Gets element from vector by index and cast it to double.
     *
     * @param i Vector element index.
     * @return Value obtained for given element index.
     */
    public double get(int i);

    /**
     * @param i Vector element index.
     * @param <T> Type of stored element in vector.
     * @return Value obtained for given element index.
     */
    public <T extends Serializable> T getRaw(int i);

    /**
     * @param i Vector element index.
     * @param v Value to set at given index.
     */
    public void set(int i, double v);

    /**
     * @param i Vector element index.
     * @param v Value to set at given index.
     */
    public void setRaw(int i, Serializable v);

    /**
     * Gets underlying array if {@link StorageOpsMetrics#isArrayBased()} returns {@code true} and all values
     * in vector are Numbers.
     * Returns {@code null} if in other cases.
     *
     * @see StorageOpsMetrics#isArrayBased()
     */
    public default double[] data() {
        return null;
    }

    /**
     * @return Underlying array of serializable objects {@link StorageOpsMetrics#isArrayBased()} returns {@code true}.
     * Returns {@code null} if in other cases.
     */
    public default Serializable[] rawData() {
        return null;
    }
}
