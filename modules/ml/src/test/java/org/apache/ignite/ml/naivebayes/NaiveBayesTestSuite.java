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

package org.apache.ignite.ml.naivebayes;

import org.apache.ignite.ml.naivebayes.compound.CompoundNaiveBayesModelTest;
import org.apache.ignite.ml.naivebayes.compound.CompoundNaiveBayesTest;
import org.apache.ignite.ml.naivebayes.compound.CompoundNaiveBayesTrainerTest;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesModelTest;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesTest;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesTrainerTest;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesModelTest;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesTest;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesTrainerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GaussianNaiveBayesModelTest.class,
    GaussianNaiveBayesTest.class,
    GaussianNaiveBayesTrainerTest.class,
    DiscreteNaiveBayesModelTest.class,
    DiscreteNaiveBayesTest.class,
    DiscreteNaiveBayesTrainerTest.class,
    CompoundNaiveBayesModelTest.class,
    CompoundNaiveBayesTest.class,
    CompoundNaiveBayesTrainerTest.class
})
public class NaiveBayesTestSuite {
}
