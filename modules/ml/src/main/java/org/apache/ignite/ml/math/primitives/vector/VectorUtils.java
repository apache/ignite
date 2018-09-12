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

package org.apache.ignite.ml.math.primitives.vector;

import java.util.Arrays;
import java.util.Objects;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;

/**
 * Some utils for {@link Vector}.
 */
public class VectorUtils {
    /** Create new vector like given vector initialized by zeroes. */
    public static Vector zeroesLike(Vector v) {
        return v.like(v.size()).assign(0.0);
    }

    /** Create new */
    public static DenseVector zeroes(int n) {
        return (DenseVector)new DenseVector(n).assign(0.0);
    }

    /**
     * Turn number into a local Vector of given size with one-hot encoding.
     *
     * @param num Number to turn into vector.
     * @param vecSize Vector size of output vector.
     * @return One-hot encoded number.
     */
    public static Vector num2Vec(int num, int vecSize) {
        return num2Vec(num, vecSize, false);
    }

    /**
     * Turn number into Vector of given size with one-hot encoding.
     *
     * @param num Number to turn into vector.
     * @param vecSize Vector size of output vector.
     * @param isDistributed Flag indicating if distributed vector should be created.
     * @return One-hot encoded number.
     */
    public static Vector num2Vec(int num, int vecSize, boolean isDistributed) {
        Vector res = new DenseVector(vecSize);
        return res.setX(num, 1);
    }

    /**
     * Turn Vector into number by looking at index of maximal element in vector.
     *
     * @param vec Vector to be turned into number.
     * @return Number.
     */
    public static double vec2Num(Vector vec) {
        int max = 0;
        double maxVal = Double.NEGATIVE_INFINITY;

        for (int i = 0; i < vec.size(); i++) {
            double curVal = vec.getX(i);
            if (curVal > maxVal) {
                max = i;
                maxVal = curVal;
            }
        }

        return max;
    }

    /**
     * Performs in-place vector multiplication.
     *
     * @param vec1 Operand to be changed and first multiplication operand.
     * @param vec2 Second multiplication operand.
     * @return Updated first operand.
     */
    public static Vector elementWiseTimes(Vector vec1, Vector vec2) {
        vec1.map(vec2, (a, b) -> a * b);

        return vec1;
    }

    /**
     * Performs in-place vector subtraction.
     *
     * @param vec1 Operand to be changed and subtracted from.
     * @param vec2 Operand to subtract.
     * @return Updated first operand.
     */
    public static Vector elementWiseMinus(Vector vec1, Vector vec2) {
        vec1.map(vec2, (a, b) -> a - b);

        return vec1;
    }

    /**
     * Zip two vectors with given binary function
     * (i.e. apply binary function to both vector elementwise and construct vector from results).
     *
     * Example zipWith({0, 2, 4}, {1, 3, 5}, plus) = {0 + 1, 2 + 3, 4 + 5}.
     * Length of result is length of shortest of vectors.
     *
     * @param v1 First vector.
     * @param v2 Second vector.
     * @param f Function to zip with.
     * @return Result of zipping.
     */
    public static Vector zipWith(Vector v1, Vector v2, IgniteBiFunction<Double, Double, Double> f) {
        int size = Math.min(v1.size(), v2.size());

        Vector res = v1.like(size);

        for (int row = 0; row < size; row++)
            res.setX(row, f.apply(v1.getX(row), v2.getX(row)));

        return res;
    }

    /**
     * Get copy of part of given length of given vector starting from given offset.
     *
     * @param v Vector to copy part from.
     * @param off Offset.
     * @param len Length.
     * @return Copy of part of given length of given vector starting from given offset.
     */
    public static Vector copyPart(Vector v, int off, int len) {
        assert off >= 0;
        assert len <= v.size();

        Vector res = v.like(len);

        for (int i = 0; i < len; i++)
            res.setX(i, v.getX(off + i));

        return res;
    }

    /**
     * Creates dense local on heap vector based on array of doubles.
     *
     * @param values Values.
     */
    public static Vector of(double ... values) {
        A.notNull(values, "values");

        return new DenseVector(values);
    }

    /**
     * Creates vector based on array of Doubles. If array contains null-elements then
     * method returns sparse local on head vector. In other case method returns
     * dense local on heap vector.
     *
     * @param values Values.
     */
    public static Vector of(Double[] values) {
        A.notNull(values, "values");

        Vector answer;
        if (Arrays.stream(values).anyMatch(Objects::isNull))
            answer = new SparseVector(values.length, StorageConstants.RANDOM_ACCESS_MODE);
        else
            answer = new DenseVector(values.length);

        for (int i = 0; i < values.length; i++)
            if (values[i] != null)
                answer.set(i, values[i]);

        return answer;
    }
}
