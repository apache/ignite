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

package org.apache.ignite.ml.tree;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.nn.performance.MnistMLPTestUtil;
import org.apache.ignite.ml.util.MnistUtils;
import org.junit.Test;

public class DecisionTreeClassificationTrainerIntegrationTest {
    @Test
    public void test() throws IOException {

        Map<Integer, MnistUtils.MnistLabeledImage> trainingSet = new HashMap<>();

        int i = 0;
        for (MnistUtils.MnistLabeledImage e : MnistMLPTestUtil.loadTrainingSet(60_000))
            trainingSet.put(i++, e);


        DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(10, 0);

        System.out.println("Start training...");
        long ts1 = System.currentTimeMillis();
        DecisionTreeNode mdl = trainer.fit(
            new LocalDatasetBuilder<>(trainingSet, 1),
            (k, v) -> v.getPixels(),
            (k, v) -> (double) v.getLabel()
        );
        long ts2 = System.currentTimeMillis();
        System.out.println("Training completed in " + (ts2 - ts1) / 1000.0 + "s");


        int correctAnswers = 0;
        int incorrectAnswers = 0;

        for (MnistUtils.MnistLabeledImage e : MnistMLPTestUtil.loadTestSet(10_000)) {
            double res = mdl.apply(e.getPixels());

            if (res == e.getLabel())
                correctAnswers++;
            else
                incorrectAnswers++;
        }

        double accuracy = 1.0 * correctAnswers / (correctAnswers + incorrectAnswers);
        System.out.println("Accuracy : " + accuracy);
    }
}
