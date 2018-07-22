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

package org.apache.ignite.ml.structures;

import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Labeled vector specialized to double label.
 *
 * @param <V> Type of vector.
 */
public class LabeledVectorDouble<V extends Vector> extends LabeledVector<V, Double> {
    /**
     * Construct LabeledVectorDouble.
     *
     * @param vector Vector.
     * @param lb Label.
     */
    public LabeledVectorDouble(V vector, Double lb) {
        super(vector, lb);
    }

    /**
     * Get label as double.
     *
     * @return label as double.
     */
    public double doubleLabel() {
        return label();
    }
}
