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

package org.apache.ignite.examples.ml.tutorial;

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
        Step_9_Go_to_LogReg.main(args);
    }
}
