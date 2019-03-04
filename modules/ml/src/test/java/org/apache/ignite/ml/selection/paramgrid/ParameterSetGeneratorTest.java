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

package org.apache.ignite.ml.selection.paramgrid;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ParameterSetGenerator}.
 */
public class ParameterSetGeneratorTest {
    /** */
    @Test
    public void testParamSetGenerator() {
        Map<Integer, Double[]> map = new TreeMap<>();
        map.put(0, new Double[]{1.1, 2.1});
        map.put(1, new Double[]{1.2, 2.2, 3.2, 4.2});
        map.put(2, new Double[]{1.3, 2.3});
        map.put(3, new Double[]{1.4});

        List<Double[]> res = new ParameterSetGenerator(map).generate();
        assertEquals(res.size(), 16);
    }
    /** */
    @Test(expected = java.lang.AssertionError.class)
    public void testParamSetGeneratorWithEmptyMap() {
        Map<Integer, Double[]> map = new TreeMap<>();
        new ParameterSetGenerator(map).generate();

    }

    /** */
    @Test(expected = java.lang.AssertionError.class)
    public void testNullHandling() {
       new ParameterSetGenerator(null).generate();
    }
}
