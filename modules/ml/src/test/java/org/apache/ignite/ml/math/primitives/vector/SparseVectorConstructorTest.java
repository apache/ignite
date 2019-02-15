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

package org.apache.ignite.ml.math.primitives.vector;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** */
public class SparseVectorConstructorTest {
    /** */
    private static final int IMPOSSIBLE_SIZE = -1;

    /** */
    @Test(expected = AssertionError.class)
    public void negativeSizeTest() {
        assertEquals("Negative size.", IMPOSSIBLE_SIZE,
            new SparseVector(-1, 1).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void zeroSizeTest() {
        assertEquals("0 size.", IMPOSSIBLE_SIZE,
            new SparseVector(0, 1).size());
    }

    /** */
    @Test
    public void primitiveTest() {
        assertEquals("1 size, random access.", 1,
            new SparseVector(1, StorageConstants.RANDOM_ACCESS_MODE).size());

        assertEquals("1 size, sequential access.", 1,
            new SparseVector(1, StorageConstants.SEQUENTIAL_ACCESS_MODE).size());

    }

    /** */
    @Test
    public void noParamsCtorTest() {
        assertNotNull(new SparseVector().nonZeroSpliterator());
    }

    /** */
    @Test
    public void mapCtorTest() {
        Map<Integer, Double> map = new HashMap<Integer, Double>() {{
            put(1, 1.);
        }};

        assertTrue("Copy true", new SparseVector(map, true).isRandomAccess());
        assertTrue("Copy false", new SparseVector(map, false).isRandomAccess());
    }
}
