/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples.ml.tutorial;

import org.apache.ignite.examples.ml.tutorial.hyperparametertuning.Step_10_RandomSearch;
import org.apache.ignite.examples.ml.tutorial.hyperparametertuning.Step_11_Parallel_BrutForce_Search;
import org.apache.ignite.examples.ml.tutorial.hyperparametertuning.Step_12_Parallel_Random_Search;
import org.apache.ignite.examples.ml.tutorial.hyperparametertuning.Step_13_Genetic_Programming_Search;
import org.apache.ignite.examples.ml.tutorial.hyperparametertuning.Step_14_Parallel_Genetic_Programming_Search;

/**
 * Run all the tutorial examples step by step with primary purpose to provide
 * automatic execution from {@code IgniteExamplesMLTestSuite}.
 */
public class TutorialStepByStepExample {
    /** Run examples with default settings. */
    public static void main(String[] args) {
        Step_1_Read_and_Learn.main(args);
        Step_2_Imputing.main(args);
        Step_3_Categorial.main(args);
        Step_3_Categorial_with_One_Hot_Encoder.main(args);
        Step_4_Add_age_fare.main(args);
        Step_5_Scaling.main(args);
        Step_6_KNN.main(args);
        Step_7_Split_train_test.main(args);
        Step_8_CV.main(args);
        Step_8_CV_with_Param_Grid.main(args);
        Step_9_Scaling_With_Stacking.main(args);
        Step_10_RandomSearch.main(args);
        Step_11_Parallel_BrutForce_Search.main(args);
        Step_12_Parallel_Random_Search.main(args);
        Step_13_Genetic_Programming_Search.main(args);
        Step_14_Parallel_Genetic_Programming_Search.main(args);
    }
}
