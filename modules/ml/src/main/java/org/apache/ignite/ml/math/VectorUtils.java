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

package org.apache.ignite.ml.math;

import java.util.Map;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.math.impls.vector.MapWrapperVector;

/**
 * Some utils for {@link Vector}.
 */
public class VectorUtils {
    /** Create new vector like given vector initialized by zeroes. */
    public static Vector zeroesLike(Vector v) {
        return v.like(v.size()).assign(0.0);
    }

    /** Create new */
    public static DenseLocalOnHeapVector zeroes(int n) {
        return (DenseLocalOnHeapVector)new DenseLocalOnHeapVector(n).assign(0.0);
    }

    /** */
    public static Vector fromMap(Map<Integer, Double> val, boolean cp) {
        return new MapWrapperVector(val);
    }

    /**
     * Turn number into Vector of given size with one-hot encoding.
     *
     * @param num Number to turn into vector.
     * @param vecSize Vector size of output vector.
     * @return One-hot encoded number.
     */
    public static Vector num2Vec(int num, int vecSize) {
        return new DenseLocalOnHeapVector(vecSize).setX(num, 1);
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
}
