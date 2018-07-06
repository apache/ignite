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

package org.apache.ignite.ml.selection.scoring.metric;

import java.util.Iterator;
import org.apache.ignite.ml.selection.scoring.LabelPair;

/**
 * Accuracy score calculator.
 *
 * @param <L> Type of a label (truth or prediction).
 */
public class Accuracy<L> implements Metric<L> {
    /** {@inheritDoc} */
    @Override public double score(Iterator<LabelPair<L>> iter) {
        long totalCnt = 0;
        long correctCnt = 0;

        while (iter.hasNext()) {
            LabelPair<L> e = iter.next();

            L prediction = e.getPrediction();
            L truth = e.getTruth();

            if (prediction.equals(truth))
                correctCnt++;

            totalCnt++;
        }

        return 1.0 * correctCnt / totalCnt;
    }
}
