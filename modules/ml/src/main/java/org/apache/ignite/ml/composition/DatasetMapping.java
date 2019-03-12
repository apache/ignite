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

package org.apache.ignite.ml.composition;

import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * This class represents dataset mapping. This is just a tuple of two mappings: one for features and one for labels.
 *
 * @param <L1> Type of labels before mapping.
 * @param <L2> Type of labels after mapping.
 */
public interface DatasetMapping<L1, L2> {
    /**
     * Method used to map feature vectors.
     *
     * @param v Feature vector.
     * @return Mapped feature vector.
     */
    public default Vector mapFeatures(Vector v) {
        return v;
    }

    /**
     * Method used to map labels.
     *
     * @param lbl Label.
     * @return Mapped label.
     */
    public L2 mapLabels(L1 lbl);

    /**
     * Dataset mapping which maps features, leaving labels unaffected.
     *
     * @param mapper Function used to map features.
     * @param <L> Type of labels.
     * @return Dataset mapping which maps features, leaving labels unaffected.
     */
    public static <L> DatasetMapping<L, L> mappingFeatures(IgniteFunction<Vector, Vector> mapper) {
        return new DatasetMapping<L, L>() {
            /** {@inheritDoc} */
            @Override public Vector mapFeatures(Vector v) {
                return mapper.apply(v);
            }

            /** {@inheritDoc} */
            @Override public L mapLabels(L lbl) {
                return lbl;
            }
        };
    }
}
