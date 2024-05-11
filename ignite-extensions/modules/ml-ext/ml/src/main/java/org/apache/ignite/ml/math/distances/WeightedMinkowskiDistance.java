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

import java.util.Arrays;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.ignite.ml.math.exceptions.math.CardinalityException;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.util.MatrixUtil;

/**
 * Calculates the Weighted Minkowski distance between two points.
 */
public class WeightedMinkowskiDistance implements DistanceMeasure {
    /**
     * Serializable version identifier.
     */
    private static final long serialVersionUID = 1771556549784040096L;

    /** */
    private int p = 1;

    /** */
    private final double[] weights;

    /** */
    @JsonIgnore
    private final Vector internalWeights;

    /** */
    @JsonCreator
    public WeightedMinkowskiDistance(@JsonProperty("p")int p, @JsonProperty("weights")double[] weights) {
        this.p = p;
        this.weights = weights.clone();
        internalWeights = VectorUtils.of(weights).copy().map(x -> Math.pow(Math.abs(x), p));
    }

    /**
     * {@inheritDoc}
     */
    @Override public double compute(Vector a, Vector b)
        throws CardinalityException {

        return Math.pow(
            MatrixUtil.localCopyOf(a).minus(b)
                .map(x -> Math.pow(Math.abs(x), p))
                .times(internalWeights)
                .sum(),
            1 / (double)p
        );
    }

    /** Returns p-norm. */
    public int getP() {
        return p;
    }

    /** Returns weights. */
    public double[] getWeights() {
        return weights.clone();
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return "WeightedMinkowskiDistance{" +
            "p=" + p +
            ", weights=" + Arrays.toString(weights) +
            '}';
    }
}
