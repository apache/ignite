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
import org.apache.ignite.ml.util.generators.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.scalar.UniformRandomProducer;
import org.apache.ignite.ml.util.generators.primitives.vector.ParametricVectorGenerator;

public class ParametricVectorGeneratorExample {
    public static void main(String... args) throws IOException {
        DataStreamGenerator spiral = new ParametricVectorGenerator(new UniformRandomProducer(-50, 50),
            t -> Math.cos(Math.abs(t)) * Math.abs(t),
            t -> Math.sin(Math.abs(t)) * Math.abs(t)
        ).asDataStream();

        Tracer.showClassificationDatasetHtml("Spiral", spiral, 20000, 0, 1, false);

        DataStreamGenerator heart = new ParametricVectorGenerator(new UniformRandomProducer(-50, 50),
            t -> 16 * Math.pow(Math.sin(t), 3),
            t -> 13 * Math.cos(t) - 5 * Math.cos(2 * t) - 2 * Math.cos(3 * t) - Math.cos(4 * t)
        ).asDataStream();

        Tracer.showClassificationDatasetHtml("Heart", heart, 2000, 0, 1, false);

        DataStreamGenerator butterfly = new ParametricVectorGenerator(new UniformRandomProducer(-100, 100),
            t -> 10 * Math.sin(t) * (Math.exp(Math.cos(t)) - 2 * Math.cos(4 * t) - Math.pow(Math.sin(t / 12), 5)),
            t -> 10 * Math.cos(t) * (Math.exp(Math.cos(t)) - 2 * Math.cos(4 * t) - Math.pow(Math.sin(t / 12), 5))
        ).asDataStream();

        Tracer.showClassificationDatasetHtml("Butterfly", butterfly, 2000, 0, 1, false);
    }
}
