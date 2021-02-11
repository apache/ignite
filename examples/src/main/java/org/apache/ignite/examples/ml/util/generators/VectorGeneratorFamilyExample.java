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

package org.apache.ignite.examples.ml.util.generators;

import java.io.IOException;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorPrimitives;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorsFamily;

/**
 * Example of using distribution families. Each distribution from family represents a class. Distribution family is a
 * distribution hence such family can be used as element of high-level family where this distribution will represent
 * one class. Such families helps to construct distributions with complex shape.
 */
public class VectorGeneratorFamilyExample {
    /**
     * Run example.
     *
     * @param args Args.
     */
    public static void main(String[] args) throws IOException {
        // Family of ring sectors.
        VectorGenerator family1 = new VectorGeneratorsFamily.Builder()
            .add(VectorGeneratorPrimitives.ring(5., 0, 2 * Math.PI))
            .add(VectorGeneratorPrimitives.ring(10., 0, Math.PI))
            .add(VectorGeneratorPrimitives.ring(15., Math.PI, 2 * Math.PI))
            .add(VectorGeneratorPrimitives.ring(20., 0, Math.PI / 2))
            .add(VectorGeneratorPrimitives.ring(25., Math.PI / 2, Math.PI))
            .add(VectorGeneratorPrimitives.ring(30., Math.PI, 3 * Math.PI / 2))
            .add(VectorGeneratorPrimitives.ring(35., 3 * Math.PI / 2, 2 * Math.PI))
            .build();

        // Family that constructed by 45 degree rotation from previous family.
        VectorGenerator family2 = family1.rotate(Math.PI / 4).map(v -> v.times(1.5));

        Tracer.showClassificationDatasetHtml("Family of ring sectors [first family]", family1.asDataStream(),
            2000, 0, 1, true);
        Tracer.showClassificationDatasetHtml("Family of ring sectors [second family]", family2.asDataStream(),
            2000, 0, 1, true);

        // Combination of families where first family represents a complex distribution for first class and
        // second family for second class.
        VectorGenerator family = new VectorGeneratorsFamily.Builder()
            .add(family1).add(family2).build();

        Tracer.showClassificationDatasetHtml("Family of ring sectors [both families as two calsses]", family.asDataStream(),
            2000, 0, 1, true);
        System.out.flush();
    }
}
