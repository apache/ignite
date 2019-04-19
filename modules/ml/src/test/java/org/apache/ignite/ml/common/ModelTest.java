/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.common;

import org.apache.ignite.ml.Model;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Tests for {@link Model} functionality.
 */
public class ModelTest {
    /** */
    @Test
    public void testCombine() {
        Model<Object, Object> mdl = new TestModel<>().combine(new TestModel<>(), (x, y) -> x);

        assertNotNull(mdl.toString(true));
        assertNotNull(mdl.toString(false));
    }

    /** */
    private static class TestModel<T, V> implements Model<T, V> {
        /** {@inheritDoc} */
        @Override public V apply(T t) {
            return null;
        }
    }
}
