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

package org.apache.ignite.ml.selection.scoring.metric;

import java.util.Arrays;
import org.apache.ignite.ml.selection.scoring.TestLabelPairCursor;
import org.apache.ignite.ml.selection.scoring.cursor.LabelPairCursor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Fmeasure}.
 */
public class FmeasureTest {
    /** */
    @Test
    public void testScore() {
        Fmeasure<Integer> scoreCalculator = new Fmeasure<>(1);

        LabelPairCursor<Integer> cursor = new TestLabelPairCursor<>(
            Arrays.asList(1, 0, 1, 0, 1, 0),
            Arrays.asList(1, 0, 0, 1, 1, 0)
        );

        double score = scoreCalculator.score(cursor.iterator());

        assertEquals((double)2/3, score, 1e-12);
    }
}
