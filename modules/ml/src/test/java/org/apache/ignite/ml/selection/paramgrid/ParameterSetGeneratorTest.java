/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
