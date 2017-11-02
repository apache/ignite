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

import org.apache.ignite.ml.math.Vector;

/**
 * Class for vector with label.
 * @param <V> Some class extending {@link Vector}.
 * @param <T> Type of label.
 */
public class LabeledVector<V extends Vector, T> {
    /** Vector. */
    private V vector;

    /** Label. */
    private T label;

    /**
     * Construct labeled vector.
     * @param vector Vector.
     * @param label Label.
     */
    public LabeledVector(V vector, T label) {
        this.vector = vector;
        this.label = label;
    }

    /**
     * Get the vector.
     * @return Vector.
     */
    public V vector() {
        return vector;
    }

    /**
     * Get the label.
     * @return Label.
     */
    public T label() {
        return label;
    }
}
