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

package org.apache.ignite.ml.math.primitives.vector.storage;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorStorage;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Common functionality for vector storage testing.
 */
public abstract class AbstractStorageTest {
    /** */
    @Test
    public void testSetAndGet() {
        VectorStorage storage = createStorage(10);
        assertTrue(isNumericVector(storage));
        assertNotNull(storage.data());

        for (int i = 0; i < storage.size(); i++) {
            storage.set(i, i);
            assertTrue(isNumericVector(storage));
            assertEquals(i, storage.get(i), 0.0);
        }
    }

    /** */
    @Test
    public void testSetAndGetRaw() {
        VectorStorage storage = createStorage(10);
        assertTrue(isNumericVector(storage));
        assertNotNull(storage.data());

        for (int i = 0; i < storage.size(); i++) {
            storage.setRaw(i, i);
            assertTrue(isRaw(storage));
            assertTrue(storage.isNumeric());
            Integer val = storage.getRaw(i);
            assertEquals(i, val.intValue());
        }
    }

    /** */
    @Test
    public void testToNumericConversion1() {
        VectorStorage storage = createStorage(10);

        storage.setRaw(0, "123");
        assertTrue(isRaw(storage));
        storage.setRaw(0, 1);
        assertEquals(1.0, storage.get(0), 0.0);
        assertTrue(isRaw(storage));
        assertTrue(storage.isNumeric());
        storage.set(0, 2);
        storage.data();
        assertEquals(2.0, storage.get(0), 0.0);
        assertTrue(isNumericVector(storage));
    }

    /** */
    @Test
    public void testToNumericConversion2() {
        VectorStorage storage = createStorage(10);
        double[] exp = new double[storage.size()];
        for (int i = 0; i < storage.size(); i++) {
            storage.setRaw(i, i);
            exp[i] = i;
        }
        assertTrue(isRaw(storage));
        assertTrue(storage.isNumeric());
        assertArrayEquals(exp, storage.data(), 0.0);
        assertTrue(isNumericVector(storage));
    }

    /** */
    @Test
    public void testCastFailure() {
        VectorStorage storage = createStorage(10);
        storage.setRaw(0, "1");
        expect(storage::data, ClassCastException.class);
        expect(() -> storage.get(0), ClassCastException.class);
        assertTrue(isRaw(storage));

        Vector v1 = createVector(10);
        Vector v2 = createVector(v1.size());
        assertTrue(isNumericVector(v1.getStorage()));
        assertTrue(isNumericVector(v2.getStorage()));

        v1.setRaw(0, "1");
        v2.setRaw(1, new HashMap<>());

        List<Function<Vector, ?>> vecOps = Arrays.asList(v1::plus, v1::dot, v1::minus, v1::times, v1::cross);
        vecOps.forEach(op -> expect(() -> op.apply(v2), ClassCastException.class));

        List<Function<Double, ?>> scalarOps = Arrays.asList(v1::plus, v1::times, v1::divide, v1::kNorm, v1::logNormalize);
        scalarOps.forEach(op -> expect(() -> op.apply(1.0), ClassCastException.class));
    }

    /** */
    protected void expect(Runnable fun, Class<? extends Exception> exCls) {
        try {
            fun.run();
        } catch (Exception ex) {
            assertEquals(exCls, ex.getClass());
            return;
        }

        throw new RuntimeException("Exception wasn't thrown");
    }

    /** */
    protected abstract boolean isNumericVector(VectorStorage storage);

    /** */
    protected abstract boolean isRaw(VectorStorage storage);

    /** */
    protected abstract VectorStorage createStorage(int size);

    /** */
    protected abstract Vector createVector(int size);
}
