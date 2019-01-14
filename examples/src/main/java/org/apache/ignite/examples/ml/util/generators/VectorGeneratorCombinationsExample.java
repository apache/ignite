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
import org.apache.ignite.ml.util.generators.primitives.scalar.UniformRandomProducer;
import org.apache.ignite.ml.util.generators.primitives.vector.ParametricVectorGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorsFamily;

public class VectorGeneratorCombinationsExample {
    public static void main(String[] args) throws IOException {
        VectorGenerator gen1 = new ParametricVectorGenerator(
            new UniformRandomProducer(-10, 10),
            t -> t,
            t -> Math.abs(t * t - 10.)
        ).filter(v -> Math.abs(v.get(0)) < 5.375);

        VectorGenerator gen2 = new ParametricVectorGenerator(
            new UniformRandomProducer(-10, 10),
            t -> t,
            t -> -Math.sqrt(10) * t * t / 8 + 30.
        ).filter(v -> Math.abs(v.get(0)) < 5.375);


        VectorGenerator family = new VectorGeneratorsFamily.Builder()
            .add(gen1).add(gen2)
            .build();

        Tracer.showClassificationDatasetHtml(family.asDataStream(), 2000, 0, 1, false);
    }
}
