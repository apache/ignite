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

package org.apache.ignite.ml.util.generators;

import org.apache.ignite.ml.util.generators.primitives.scalar.DiscreteRandomProducerTest;
import org.apache.ignite.ml.util.generators.primitives.scalar.GaussRandomProducerTest;
import org.apache.ignite.ml.util.generators.primitives.scalar.RandomProducerTest;
import org.apache.ignite.ml.util.generators.primitives.scalar.UniformRandomProducerTest;
import org.apache.ignite.ml.util.generators.primitives.vector.ParametricVectorGeneratorTest;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorPrimitivesTest;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorTest;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGeneratorsFamilyTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all tests located in {@link org.apache.ignite.ml.util.generators} package.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    DiscreteRandomProducerTest.class,
    GaussRandomProducerTest.class,
    RandomProducerTest.class,
    UniformRandomProducerTest.class,
    ParametricVectorGeneratorTest.class,
    VectorGeneratorPrimitivesTest.class,
    VectorGeneratorsFamilyTest.class,
    VectorGeneratorTest.class,
    DataStreamGeneratorTest.class,
    DataStreamGeneratorFillCacheTest.class
})
public class DataStreamGeneratorTestSuite {
}


