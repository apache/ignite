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

package org.apache.ignite.examples.ml.knn;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.encoding.EncoderTrainer;
import org.apache.ignite.ml.preprocessing.encoding.EncoderType;

public class KNNRegressionExample1 {
    /** Run example. */
    public static void main(String[] args) throws FileNotFoundException {
        Map<Integer, Object[]> training = new HashMap<>();

        training.put(0, new Object[] {42.0});
        training.put(1, new Object[] {43.0});
        training.put(2, new Object[] {42.0});

        EncoderTrainer<Integer, Object[]> trainer = new EncoderTrainer<Integer, Object[]>()
            .withEncoderType(EncoderType.ONE_HOT_ENCODER)
            .withEncodedFeature(0);

        IgniteBiFunction<Integer, Object[], Vector> processor = trainer.fit(training, 1, (k, v) -> v);
        Vector res = processor.apply(1, new Object[] {42.0});

        System.out.println(Arrays.toString(res.asArray()));
    }
}
