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

package org.apache.ignite.ml.knn.utils.indices;

import java.util.List;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * An index that works with spatial data and allows to quickly find {@code k} closest element.
 *
 * @param <L> Label type.
 */
public interface SpatialIndex<L> extends AutoCloseable {
    /**
     * Finds {@code k} closest elements to the specified point.
     *
     * @param k Number of elements to be returned.
     * @param pnt Point to be used to calculate distance to other points.
     * @return An array of the {@code k} closest elements to the specified point.
     */
    public List<LabeledVector<L>> findKClosest(int k, Vector pnt);

    /** {@inheritDoc} */
    @Override public default void close() {
        // No-op.
    }
}
