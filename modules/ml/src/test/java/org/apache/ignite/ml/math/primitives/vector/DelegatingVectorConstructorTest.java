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

import org.apache.ignite.ml.math.primitives.vector.impl.DelegatingVector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** */
public class DelegatingVectorConstructorTest {
    /** */
    private static final int IMPOSSIBLE_SIZE = -1;

    /** */
    @Test
    public void basicTest() {
        final Vector parent = new DenseVector(new double[] {0, 1});

        final Vector delegate = new DelegatingVector(parent);

        final int size = parent.size();

        assertEquals("Delegate size differs from expected.", size, delegate.size());

        for (int idx = 0; idx < size; idx++)
            assertDelegate(parent, delegate, idx);
    }

    /** */
    private void assertDelegate(Vector parent, Vector delegate, int idx) {
        assertValue(parent, delegate, idx);

        parent.set(idx, parent.get(idx) + 1);

        assertValue(parent, delegate, idx);

        delegate.set(idx, delegate.get(idx) + 2);

        assertValue(parent, delegate, idx);
    }

    /** */
    private void assertValue(Vector parent, Vector delegate, int idx) {
        assertEquals("Unexpected value at index " + idx, parent.get(idx), delegate.get(idx), 0d);
    }
}
