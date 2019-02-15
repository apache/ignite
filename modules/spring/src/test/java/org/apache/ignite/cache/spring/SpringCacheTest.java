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

package org.apache.ignite.cache.spring;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link SpringCache}
 */
@RunWith(JUnit4.class)
public class SpringCacheTest extends GridCommonAbstractTest {
    /** */
    private static Ignite ignite;

    /** Wrapped cache. */
    private IgniteCache nativeCache;

    /** Working cache. */
    private SpringCache springCache;

    /** */
    private String cacheName;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        G.stop(true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cacheName = String.valueOf(System.currentTimeMillis());
        nativeCache = ignite.getOrCreateCache(cacheName);
        springCache = new SpringCache(nativeCache, null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        ignite.destroyCache(cacheName);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetName() throws Exception {
        assertEquals(cacheName, springCache.getName());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetNativeCache() throws Exception {
        assertEquals(nativeCache, springCache.getNativeCache());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetByKey() throws Exception {
        String key = "key";
        String value = "value";

        springCache.put(key, value);
        assertEquals(value, springCache.get(key).get());

        assertNull(springCache.get("wrongKey"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetByKeyType() throws Exception {
        String key = "key";
        String value = "value";

        springCache.put(key, value);
        assertEquals(value, springCache.get(key, String.class));

        try {
            springCache.get(key, Integer.class);
            fail("Missing exception");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Cached value is not of required type [cacheName=" + cacheName));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPut() throws Exception {
        String key = "key";
        assertNull(springCache.get(key));

        String value = "value";
        springCache.put(key, value);

        assertEquals(value, springCache.get(key).get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutIfAbsent() throws Exception {
        String key = "key";
        String expected = "value";

        assertNull(springCache.putIfAbsent(key, expected));

        assertEquals(expected, springCache.putIfAbsent(key, "wrongValue").get());

        assertEquals(expected, springCache.get(key).get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEvict() throws Exception {
        String key = "key";
        assertNull(springCache.get(key));

        springCache.put(key, "value");
        assertNotNull(springCache.get(key));

        springCache.evict(key);
        assertNull(springCache.get(key));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClear() throws Exception {
        String key;
        springCache.put((key = "key1"), "value1");
        assertNotNull(springCache.get(key));
        springCache.put((key = "key2"), "value2");
        assertNotNull(springCache.get(key));
        springCache.put((key = "key3"), "value3");
        assertNotNull(springCache.get(key));

        springCache.clear();

        assertNull(springCache.get("key1"));
        assertNull(springCache.get("key2"));
        assertNull(springCache.get("key3"));
    }
}
