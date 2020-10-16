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

package org.apache.ignite.examples;

import org.apache.ignite.examples.ml.tutorial.Step_10_Bagging;
import org.apache.ignite.examples.ml.tutorial.Step_11_Boosting;
import org.apache.ignite.examples.ml.tutorial.Step_12_Model_Update;
import org.apache.ignite.examples.ml.tutorial.Step_1_Read_and_Learn;
import org.apache.ignite.examples.ml.tutorial.Step_2_Imputing;
import org.apache.ignite.examples.ml.tutorial.Step_3_Categorial;
import org.apache.ignite.examples.ml.tutorial.Step_3_Categorial_with_One_Hot_Encoder;
import org.apache.ignite.examples.ml.tutorial.Step_4_Add_age_fare;
import org.apache.ignite.examples.ml.tutorial.Step_5_Scaling;
import org.apache.ignite.examples.ml.tutorial.Step_6_KNN;
import org.apache.ignite.examples.ml.tutorial.Step_7_Split_train_test;
import org.apache.ignite.examples.ml.tutorial.Step_8_CV;
import org.apache.ignite.examples.ml.tutorial.Step_8_CV_with_Param_Grid;
import org.apache.ignite.examples.ml.tutorial.Step_8_CV_with_Param_Grid_and_pipeline;
import org.apache.ignite.examples.ml.tutorial.Step_9_Scaling_With_Stacking;
import org.apache.ignite.examples.ml.tutorial.hyperparametertuning.Step_13_RandomSearch;
import org.apache.ignite.examples.ml.tutorial.hyperparametertuning.Step_14_Parallel_Brute_Force_Search;
import org.apache.ignite.examples.ml.tutorial.hyperparametertuning.Step_15_Parallel_Random_Search;
import org.apache.ignite.examples.ml.tutorial.hyperparametertuning.Step_16_Genetic_Programming_Search;
import org.apache.ignite.examples.ml.tutorial.hyperparametertuning.Step_17_Parallel_Genetic_Programming_Search;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;
import org.junit.Test;

/**
 * ML Titanic Tutorial self test.
 */
public class MLTutorialExamplesSelfTest extends GridAbstractExamplesTest {
    /** */
    @Test
    public void testTutorialStep1ReadAndLearnExample() {
        Step_1_Read_and_Learn.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep2ImputingExample() {
        Step_2_Imputing.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep3CategorialExample() {
        Step_3_Categorial.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep3CategorialWithOneHotEncoderExample() {
        Step_3_Categorial_with_One_Hot_Encoder.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep4AddAgeFareExample() {
        Step_4_Add_age_fare.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep5ScalingExample() {
        Step_5_Scaling.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep6KnnExample() {
        Step_6_KNN.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep7Example() {
        Step_7_Split_train_test.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep8CvExample() {
        Step_8_CV.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep8CvWithParamGridExample() {
        Step_8_CV_with_Param_Grid.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep8CvWithParamGridAndPipelineExample() {
        Step_8_CV_with_Param_Grid_and_pipeline.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep9ScalingWithStackingExample() {
        Step_9_Scaling_With_Stacking.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep10BaggingExample() {
        Step_10_Bagging.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep11BoostingExample() {
        Step_11_Boosting.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep12ModelUpdateExample() {
        Step_12_Model_Update.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep13RandomSearchExample() {
        Step_13_RandomSearch.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep14BruteForceExample() {
        Step_14_Parallel_Brute_Force_Search.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep15ParallelRandomSearchExample() {
        Step_15_Parallel_Random_Search.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep16GeneticProgrammingSearchExample() {
        Step_16_Genetic_Programming_Search.main(EMPTY_ARGS);
    }

    /** */
    @Test
    public void testTutorialStep17ParallelGeneticProgrammingSearchExample() {
        Step_17_Parallel_Genetic_Programming_Search.main(EMPTY_ARGS);
    }
}
