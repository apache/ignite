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

package org.apache.ignite.internal.processors.cache.multijvm;

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.util.typedef.internal.*;

/**
 * Multy Jvm tests.
 */
public class GridCachePartitionedMultiJvmFullApiSelfTest extends GridCachePartitionedMultiNodeFullApiSelfTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        IgniteExProcessProxy.killAll(); // TODO: remove processes killing from here.
    }

    /** {@inheritDoc} */
    protected boolean isMultiJvm() {
        return true;
    }

    @Override protected boolean isMultiJvmApplicable(String testName) {
        return "testPutAllRemoveAll".equals(testName);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllRemoveAll() throws Exception {
        super.testPutAllRemoveAll();
    }

    public void testPutAllPutAll() throws Exception {
        super.testPutAllPutAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutRemove() throws Exception {
        IgniteCache<Object, Object> c0 = grid(0).cache(null);
        IgniteCache<Object, Object> c1 = grid(1).cache(null);

        final int key = 1;
        final int val = 3;

        c0.put(key, val);

        assertEquals(val, c0.get(key));
        assertEquals(val, c1.get(key));

        assertTrue(c1.remove(key));

        U.sleep(1_000);

        assertTrue(c0.get(key) == null || c1.get(key) == null);
        assertNull(c1.get(key));
        assertNull(c0.get(key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutRemove2() throws Exception {
        IgniteCache<Object, Object> c0 = grid(0).cache(null);
        IgniteCache<Object, Object> c1 = grid(1).cache(null);

        final int key = 1;
        final int val = 3;

        c1.put(key, val);

        assertEquals(val, c1.get(key));
        assertEquals(val, c0.get(key));

        assertTrue(c0.remove(key));

        U.sleep(1_000);

        assertNull(c1.get(key));
        assertNull(c0.get(key));
    }
}
