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

package org.apache.ignite.ml.selection.split;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link TrainTestDatasetSplitter}.
 */
public class TrainTestDatasetSplitterTest {
    /** */
    @Test
    public void testSplitWithSpecifiedTrainAndTestSize() {
        TrainTestDatasetSplitter<Double, Double> splitter = new TrainTestDatasetSplitter<>((k, v) -> k);

        TrainTestSplit<Double, Double> split = splitter.split(0.4, 0.4);

        assertTrue(split.getTrainFilter().apply(0.0, 0.0));
        assertTrue(split.getTrainFilter().apply(0.2, 0.0));
        assertFalse(split.getTrainFilter().apply(0.4, 0.0));
        assertFalse(split.getTrainFilter().apply(0.6, 0.0));

        assertFalse(split.getTestFilter().apply(0.0, 0.0));
        assertFalse(split.getTestFilter().apply(0.2, 0.0));
        assertTrue(split.getTestFilter().apply(0.4, 0.0));
        assertTrue(split.getTestFilter().apply(0.6, 0.0));
    }
}
