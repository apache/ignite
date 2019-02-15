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

package org.apache.ignite.ml.math.primitives.matrix;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.ignite.ml.math.ExternalizeTest;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link MatrixStorage} implementations.
 *
 * TODO: IGNITE-5723, add attribute tests.
 */
public class MatrixStorageImplementationTest extends ExternalizeTest<MatrixStorage> {
    /**
     * The columnSize() and the rowSize() test.
     */
    @Test
    public void sizeTest() {
        final AtomicReference<Integer> expRowSize = new AtomicReference<>(0);
        final AtomicReference<Integer> expColSize = new AtomicReference<>(0);

        consumeSampleStorages((x, y) -> {
                expRowSize.set(x);
                expColSize.set(y);
            },
            (ms, desc) -> assertTrue("Expected size for " + desc, expColSize.get().equals(ms.columnSize()) && expRowSize.get().equals(ms.rowSize())));
    }

    /** */
    @Test
    public void getSetTest() {
        consumeSampleStorages(null, (ms, desc) -> {
            for (int i = 0; i < ms.rowSize(); i++) {
                for (int j = 0; j < ms.columnSize(); j++) {
                    double random = Math.random();
                    ms.set(i, j, random);
                    assertTrue("Unexpected value for " + desc + " x:" + i + ", y:" + j, Double.compare(random, ms.get(i, j)) == 0);
                }
            }
        });
    }

    /** */
    @Override public void externalizeTest() {
        consumeSampleStorages(null, (ms, desc) -> externalizeTest(ms));
    }

    /** */
    private void consumeSampleStorages(BiConsumer<Integer, Integer> paramsConsumer,
        BiConsumer<MatrixStorage, String> consumer) {
        new MatrixStorageFixtures().consumeSampleStorages(paramsConsumer, consumer);
    }
}
