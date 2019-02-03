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

package org.apache.ignite.ml.selection.scoring.cursor;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.selection.scoring.LabelPair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LocalLabelPairCursor}.
 */
public class LocalLabelPairCursorTest {
    /** */
    @Test
    public void testIterate() {
        Map<Integer, Integer> data = new HashMap<>();

        for (int i = 0; i < 1000; i++)
            data.put(i, i);

        LabelPairCursor<Integer> cursor = new LocalLabelPairCursor<>(
            data,
            (k, v) -> v % 2 == 0,
            (k, v) -> VectorUtils.of(v),
            (k, v) -> v,
            vec -> (int)vec.get(0)
        );

        int cnt = 0;
        for (LabelPair<Integer> e : cursor) {
            assertEquals(e.getPrediction(), e.getTruth());
            cnt++;
        }
        assertEquals(500, cnt);
    }
}
