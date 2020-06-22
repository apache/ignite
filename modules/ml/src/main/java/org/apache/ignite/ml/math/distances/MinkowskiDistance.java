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

import java.util.Objects;
import org.apache.ignite.ml.math.exceptions.math.CardinalityException;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.util.MatrixUtil;

import static org.apache.ignite.ml.math.functions.Functions.PLUS;

/**
 * Calculates the L<sub>p</sub> (Minkowski) distance between two points.
 */
public class MinkowskiDistance implements DistanceMeasure {
    /** Serializable version identifier. */
    private static final long serialVersionUID = 1717556319784040040L;

    /** Distance paramenter. */
    private final double p;

    /** @param p norm */
    public MinkowskiDistance(double p) {
        this.p = p;
    }

    /** {@inheritDoc} */
    @Override public double compute(Vector a, Vector b) throws CardinalityException {
        assert a.size() == b.size();
        IgniteDoubleFunction<Double> fun = value -> Math.pow(Math.abs(value), p);

        Double result = MatrixUtil.localCopyOf(a).minus(b).foldMap(PLUS, fun, 0d);
        return Math.pow(result, 1 / p);
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        MinkowskiDistance that = (MinkowskiDistance)o;
        return Double.compare(that.p, p) == 0;
    }

    @Override public int hashCode() {
        return Objects.hash(p);
    }
}
