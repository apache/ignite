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

package org.apache.ignite.ml.selection.split.mapper;

import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Test for {@link SHA256UniformMapper}.
 */
public class SHA256UniformMapperTest {
    /** */
    @Test
    public void testMap() {
        UniformMapper<Integer, Integer> mapper = new SHA256UniformMapper<>(new Random(42));

        int cnt = 0;

        for (int i = 0; i < 100_000; i++) {
            double pnt = mapper.map(i, i);

            if (pnt < 0.2)
                cnt++;
        }

        double err = 1.0 * Math.abs(cnt - 20_000) / 20_000;

        // Hash function should provide a good distribution so that error should be less that 2% in case 10^5 tests.
        assertTrue(err < 0.02);
    }

    /** */
    @Test
    public void testMapAndMapAgain() {
        UniformMapper<Integer, Integer> firstMapper = new SHA256UniformMapper<>(new Random(42));
        UniformMapper<Integer, Integer> secondMapper = new SHA256UniformMapper<>(new Random(21));

        int cnt = 0;

        for (int i = 0; i < 100_000; i++) {
            double firstPnt = firstMapper.map(i, i);
            double secondPnt = secondMapper.map(i, i);

            if (firstPnt < 0.5 && secondPnt < 0.5)
                cnt++;
        }

        double err = 1.0 * Math.abs(cnt - 25_000) / 25_000;

        // Hash function should provide a good distribution so that error should be less that 2% in case 10^5 tests.
        assertTrue(err < 0.02);
    }
}
