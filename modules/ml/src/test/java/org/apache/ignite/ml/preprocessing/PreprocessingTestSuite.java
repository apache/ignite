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

package org.apache.ignite.ml.preprocessing;

import org.apache.ignite.ml.preprocessing.binarization.BinarizationPreprocessorTest;
import org.apache.ignite.ml.preprocessing.binarization.BinarizationTrainerTest;
import org.apache.ignite.ml.preprocessing.encoding.EncoderTrainerTest;
import org.apache.ignite.ml.preprocessing.encoding.FrequencyEncoderPreprocessorTest;
import org.apache.ignite.ml.preprocessing.encoding.LabelEncoderPreprocessorTest;
import org.apache.ignite.ml.preprocessing.encoding.OneHotEncoderPreprocessorTest;
import org.apache.ignite.ml.preprocessing.encoding.StringEncoderPreprocessorTest;
import org.apache.ignite.ml.preprocessing.imputing.ImputerPreprocessorTest;
import org.apache.ignite.ml.preprocessing.imputing.ImputerTrainerTest;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerPreprocessorTest;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainerTest;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationPreprocessorTest;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all tests located in org.apache.ignite.ml.preprocessing.* package.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    MinMaxScalerPreprocessorTest.class,
    MinMaxScalerTrainerTest.class,
    BinarizationPreprocessorTest.class,
    BinarizationTrainerTest.class,
    ImputerPreprocessorTest.class,
    ImputerTrainerTest.class,
    EncoderTrainerTest.class,
    OneHotEncoderPreprocessorTest.class,
    FrequencyEncoderPreprocessorTest.class,
    StringEncoderPreprocessorTest.class,
    LabelEncoderPreprocessorTest.class,
    NormalizationTrainerTest.class,
    NormalizationPreprocessorTest.class

})
public class PreprocessingTestSuite {
    // No-op.
}
