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

import org.apache.ignite.ml.math.ExternalizeTest;
import org.apache.ignite.ml.math.primitives.MathTestConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Abstract class with base tests for each matrix storage.
 */
public abstract class MatrixBaseStorageTest<T extends MatrixStorage> extends ExternalizeTest<T> {
    /** */
    protected T storage;

    /** */
    @Before
    public abstract void setUp();

    /** */
    @After
    public void tearDown() throws Exception {
        storage.destroy();
    }

    /** */
    @Test
    public void getSet() throws Exception {
        int rows = MathTestConstants.STORAGE_SIZE;
        int cols = MathTestConstants.STORAGE_SIZE;

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                double data = Math.random();

                storage.set(i, j, data);

                Assert.assertEquals(MathTestConstants.VAL_NOT_EQUALS, storage.get(i, j), data, MathTestConstants.NIL_DELTA);
            }
        }
    }

    /** */
    @Test
    public void columnSize() throws Exception {
        assertEquals(MathTestConstants.VAL_NOT_EQUALS, storage.columnSize(), MathTestConstants.STORAGE_SIZE);
    }

    /** */
    @Test
    public void rowSize() throws Exception {
        assertEquals(MathTestConstants.VAL_NOT_EQUALS, storage.rowSize(), MathTestConstants.STORAGE_SIZE);
    }

    /** */
    @Override public void externalizeTest() {
        fillMatrix();
        super.externalizeTest(storage);
    }

    /** */
    protected void fillMatrix() {
        for (int i = 0; i < storage.rowSize(); i++) {
            for (int j = 0; j < storage.columnSize(); j++)
                storage.set(i, j, Math.random());
        }
    }
}
