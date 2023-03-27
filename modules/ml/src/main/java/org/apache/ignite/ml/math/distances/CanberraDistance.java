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
package org.apache.ignite.ml.math.distances;

import org.apache.ignite.ml.math.exceptions.math.CardinalityException;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.util.MatrixUtil;

/**
 * Calculates the Canberra distance between two points.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Canberra_distance">Canberra distance</a>
 */
public class CanberraDistance implements DistanceMeasure {
    /**
     * Serializable version identifier.
     */
    private static final long serialVersionUID = 1771556549784040092L;

    /**
     * {@inheritDoc}
     */
    @Override public double compute(Vector a, Vector b)
        throws CardinalityException {
        Vector top = MatrixUtil.localCopyOf(a).minus(b).map(Math::abs);
        Vector down = MatrixUtil.localCopyOf(a).map(Math::abs)
            .plus(MatrixUtil.localCopyOf(b).map(Math::abs))
            .map(value -> value != 0 ? 1 / value : 0);

        return top.times(down).sum();
    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        return obj != null && getClass() == obj.getClass();
    }

    /**
     * {@inheritDoc}
     */
    @Override public int hashCode() {
        return getClass().hashCode();
    }
}
