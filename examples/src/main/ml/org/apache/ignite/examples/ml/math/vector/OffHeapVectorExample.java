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

package org.apache.ignite.examples.ml.math.vector;

import java.util.Arrays;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOffHeapVector;

/**
 * This example shows how to create and use off-heap versions of {@link Vector}.
 */
public final class OffHeapVectorExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        System.out.println();
        System.out.println(">>> Off-heap vector API usage example started.");

        System.out.println("\n>>> Creating perpendicular off-heap vectors.");
        double[] data1 = new double[] {1, 0, 3, 0, 5, 0};
        double[] data2 = new double[] {0, 2, 0, 4, 0, 6};

        Vector v1 = new DenseLocalOffHeapVector(data1.length);
        Vector v2 = new DenseLocalOffHeapVector(data2.length);

        v1.assign(data1);
        v2.assign(data2);

        System.out.println(">>> First vector: " + Arrays.toString(data1));
        System.out.println(">>> Second vector: " + Arrays.toString(data2));

        double dotProduct = v1.dot(v2);
        boolean dotProductIsAsExp = dotProduct == 0;

        System.out.println("\n>>> Dot product of vectors: [" + dotProduct
            + "], it is 0 as expected: [" + dotProductIsAsExp + "].");

        assert dotProductIsAsExp : "Expect dot product of perpendicular vectors to be 0.";

        Vector hypotenuse = v1.plus(v2);

        System.out.println("\n>>> Hypotenuse (sum of vectors): " + Arrays.toString(hypotenuse.getStorage().data()));

        double lenSquared1 = v1.getLengthSquared();
        double lenSquared2 = v2.getLengthSquared();
        double lenSquaredHypotenuse = hypotenuse.getLengthSquared();

        boolean lenSquaredHypotenuseIsAsExp = lenSquaredHypotenuse == lenSquared1 + lenSquared2;

        System.out.println(">>> Squared length of first vector: [" + lenSquared1 + "].");
        System.out.println(">>> Squared length of second vector: [" + lenSquared2 + "].");
        System.out.println(">>> Squared length of hypotenuse: [" + lenSquaredHypotenuse
            + "], equals sum of squared lengths of two original vectors as expected: ["
            + lenSquaredHypotenuseIsAsExp + "].");

        assert lenSquaredHypotenuseIsAsExp : "Expect squared length of hypotenuse to be as per Pythagorean theorem.";

        System.out.println("\n>>> Off-heap vector API usage example completed.");
    }
}
