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

package org.apache.ignite.ml.selection.split;

import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link TrainTestDatasetSplitter}.
 */
public class TrainTestDatasetSplitterTest {
    /** */
    @Test
    public void testSplitWithSpecifiedTrainAndTestSize() {
        TrainTestDatasetSplitter<Double, Double> splitter = new TrainTestDatasetSplitter<>((k, v) -> k);

        TrainTestSplit<Double, Double> split = splitter.split(0.4, 0.4);

        assertEquals(0, split.getTrainFilter().getFrom(), 1e-12);
        assertEquals(0.4, split.getTrainFilter().getTo(), 1e-12);
        assertEquals(0.4, split.getTestFilter().getFrom(), 1e-12);
        assertEquals(0.8, split.getTestFilter().getTo(), 1e-12);
    }

    /** */
    @Test
    public void testSplitWithSpecifiedTrainSize() {
        TrainTestDatasetSplitter<Double, Double> splitter = new TrainTestDatasetSplitter<>((k, v) -> k);

        TrainTestSplit<Double, Double> split = splitter.split(0.4);

        assertEquals(0, split.getTrainFilter().getFrom(), 1e-12);
        assertEquals(0.4, split.getTrainFilter().getTo(), 1e-12);
        assertEquals(0.4, split.getTestFilter().getFrom(), 1e-12);
        assertEquals(1.0, split.getTestFilter().getTo(), 1e-12);
    }
}
