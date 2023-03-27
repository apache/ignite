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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.ignite.ml.math.exceptions.math.CardinalityException;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;

/**
 * This class is based on the corresponding class from Apache Common Math lib. Interface for distance measures of
 * n-dimensional vectors.
 */
@JsonTypeInfo( use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = BrayCurtisDistance.class, name = "BrayCurtisDistance"),
    @JsonSubTypes.Type(value = CanberraDistance.class, name = "CanberraDistance"),
    @JsonSubTypes.Type(value = ChebyshevDistance.class, name = "ChebyshevDistance"),
    @JsonSubTypes.Type(value = CosineSimilarity.class, name = "CosineSimilarity"),
    @JsonSubTypes.Type(value = EuclideanDistance.class, name = "EuclideanDistance"),
    @JsonSubTypes.Type(value = HammingDistance.class, name = "HammingDistance"),
    @JsonSubTypes.Type(value = JaccardIndex.class, name = "JaccardIndex"),
    @JsonSubTypes.Type(value = JensenShannonDistance.class, name = "JensenShannonDistance"),
    @JsonSubTypes.Type(value = ManhattanDistance.class, name = "ManhattanDistance"),
    @JsonSubTypes.Type(value = MinkowskiDistance.class, name = "MinkowskiDistance"),
    @JsonSubTypes.Type(value = WeightedMinkowskiDistance.class, name = "WeightedMinkowskiDistance"),
})
public interface DistanceMeasure extends Externalizable {
    /**
     * Compute the distance between two n-dimensional vectors.
     * <p>
     * The two vectors are required to have the same dimension.
     *
     * @param a The first vector.
     * @param b The second vector.
     * @return The distance between the two vectors.
     * @throws CardinalityException if the array lengths differ.
     */
    public double compute(Vector a, Vector b) throws CardinalityException;

    /**
     * Compute the distance between n-dimensional vector and n-dimensional array.
     * <p>
     * The two data structures are required to have the same dimension.
     *
     * @param a The vector.
     * @param b The array.
     * @return The distance between vector and array.
     * @throws CardinalityException if the data structures lengths differ.
     */
    default double compute(Vector a, double[] b) throws CardinalityException {
        return compute(a, new DenseVector(b));
    }

    /** {@inheritDoc} */
    @Override default void writeExternal(ObjectOutput out) throws IOException {
        // No-op
    }

    /** {@inheritDoc} */
    @Override default void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op
    }
}
