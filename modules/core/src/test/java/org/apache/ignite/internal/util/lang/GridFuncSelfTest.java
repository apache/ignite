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

package org.apache.ignite.internal.util.lang;

import java.util.TreeMap;
import org.junit.Test;

import static org.apache.ignite.internal.util.lang.ClusterNodeFunc.eqNotOrdered;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** */
public class GridFuncSelfTest {
    /** */
    @Test
    public void testMapEqNotOrdered() {
        String str = "mystring";

        TreeMap<String, Object> m1 = new TreeMap<>();

        TreeMap<String, Object> m2 = new TreeMap<>();

        m1.put("1", str);
        m2.put("1", str);

        m1.put("2", "2");
        m2.put("3", "3");

        assertFalse(eqNotOrdered(m1, m2));

        m1.remove("2");
        m2.remove("3");

        m1.put("arr", new byte[] {1, 2, 3});
        m2.put("arr", new byte[] {1, 2, 3});

        assertTrue(eqNotOrdered(m1, m2));
    }
}
