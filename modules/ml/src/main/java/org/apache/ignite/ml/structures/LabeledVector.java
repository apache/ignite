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

import java.io.Serializable;
import org.apache.ignite.ml.math.Vector;

/**
 * Class for vector with label.
 *
 * @param <V> Some class extending {@link Vector}.
 * @param <T> Type of label.
 */
public class LabeledVector<V extends Vector, T> implements Serializable {
    /** Vector. */
    private final V vector;

    /** Label. */
    private T lb;

    /**
     * Construct labeled vector.
     *
     * @param vector Vector.
     * @param lb Label.
     */
    public LabeledVector(V vector, T lb) {
        this.vector = vector;
        this.lb = lb;
    }

    /**
     * Get the vector.
     *
     * @return Vector.
     */
    public V features() {
        return vector;
    }

    /**
     * Get the label.
     *
     * @return Label.
     */
    public T label() {
        return lb;
    }

    /**
     * Set the label
     *
     * @param lb Label.
     */
    public void setLabel(T lb) {
        this.lb = lb;
    }

    /** */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        LabeledVector vector1 = (LabeledVector)o;

        if (vector != null ? !vector.equals(vector1.vector) : vector1.vector != null)
            return false;
        return lb != null ? lb.equals(vector1.lb) : vector1.lb == null;
    }

    /** */
    @Override public int hashCode() {
        int res = vector != null ? vector.hashCode() : 0;
        res = 31 * res + (lb != null ? lb.hashCode() : 0);
        return res;
    }
}
