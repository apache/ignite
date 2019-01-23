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

package org.apache.ignite.ml.math.primitives.vector;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;
import org.apache.ignite.ml.math.primitives.vector.impl.SimpleNamedVector;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

/**
 * Unit test for {@link SimpleNamedVector}.
 */
public class SimpleNamedVectorTest {
    /** */
    @Test
    public void testAdd() {
        Map<String, Double> data = new HashMap<>();
        data.put("a", 42.0);

        SimpleNamedVector a = VectorUtils.of(data);
        SimpleNamedVector b = VectorUtils.of(data);
        SimpleNamedVector c = a.plus(b);

        assertEquals(1, c.size());
        assertEquals(84.0, c.get("a"));
        assertEquals(84.0, c.get(0));
    }

    /** */
    @Test(expected = MathIllegalArgumentException.class)
    public void testAddWithDifferentVectorMappings() {
        Map<String, Double> data = new HashMap<>();
        data.put("a", 42.0);

        SimpleNamedVector a = VectorUtils.of(data);

        data.remove("a");
        data.put("b", 42.0);

        SimpleNamedVector b = VectorUtils.of(data);

        a.plus(b);
    }
}
