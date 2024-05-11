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
 * Calculates the JensenShannonDistance distance between two points.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Jensen%E2%80%93Shannon_divergence">
 *   Jensen–Shannon divergence</a>
 */
public class JensenShannonDistance implements DistanceMeasure {
    /**
     * Serializable version identifier.
     */
    private static final long serialVersionUID = 1771556549784040093L;

    /** */
    private final Double base;

    /** */
    public JensenShannonDistance() {
        base = Math.E;
    }

    /** */
    public JensenShannonDistance(Double base) {
        this.base = base;
    }

    /**
     * {@inheritDoc}
     */
    @Override public double compute(Vector a, Vector b)
        throws CardinalityException {
        Vector aNormalized = MatrixUtil.localCopyOf(a).divide(a.sum());
        Vector bNormalized = MatrixUtil.localCopyOf(b).divide(b.sum());

        Vector mean = aNormalized.plus(bNormalized).divide(2d);

        double js = aNormalized.map(mean, this::relativeEntropy).sum() +
            bNormalized.map(mean, this::relativeEntropy).sum();

        js /= Math.log(base);

        return Math.sqrt(js / 2d);
    }

    /** */
    private double relativeEntropy(double x, double y) {
        if (x > 0 && y > 0) {
            return x * Math.log(x / y);
        }
        if (x == 0 && y >= 0) {
            return 0;
        }

        return Double.POSITIVE_INFINITY;
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
