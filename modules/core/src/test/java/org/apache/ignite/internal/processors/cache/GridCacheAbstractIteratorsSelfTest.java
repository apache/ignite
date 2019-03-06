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

package org.apache.ignite.internal.processors.cache;

import javax.cache.Cache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.junit.Assume;
import org.junit.Test;

/**
 * Tests for cache iterators.
 */
public abstract class GridCacheAbstractIteratorsSelfTest extends GridCacheAbstractSelfTest {
    /** Key prefix. */
    protected static final String KEY_PREFIX = "testKey";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        for (int i = 0; i < entryCount(); i++)
            jcache().put(KEY_PREFIX + i, i);
    }

    /**
     * @return Entry count.
     */
    protected abstract int entryCount();

    /** */
    @Test
    public void testCacheIterator() {
        int cnt = 0;

        for (Cache.Entry<String, Integer> entry : jcache()) {
            assert entry != null;
            assert entry.getKey() != null;
            assert entry.getKey().contains(KEY_PREFIX);
            assert entry.getValue() != null;
            assert entry.getValue() >= 0 && entry.getValue() < entryCount();
            assert entry.getValue() != null;
            assert entry.getValue() >= 0 && entry.getValue() < entryCount();

            cnt++;
        }

        assertEquals(cnt, entryCount());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCacheIteratorMultithreaded() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            jcache(i).removeAll();

        final IgniteInternalFuture<?> putFut = GridTestUtils.runMultiThreadedAsync(new CAX() {
            @Override public void applyx() {
                for (int i = 0; i < entryCount(); i++)
                    jcache().put(KEY_PREFIX + i, i);
            }
        }, 1, "put-thread");

        GridTestUtils.runMultiThreaded(new CA() {
            @Override public void apply() {
                while (!putFut.isDone()) {
                    for (Cache.Entry<String, Integer> entry : jcache()) {
                        assert entry != null;
                        assert entry.getKey() != null;
                        assert entry.getKey().contains(KEY_PREFIX);
                    }
                }
            }
        }, 3, "iterator-thread");
    }

    /** */
    @Test
    public void testEntrySetIterator() {
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-10082", MvccFeatureChecker.forcedMvcc());

        assert jcache().localSize(CachePeekMode.ALL) == entryCount();

        int cnt = 0;

        for (Cache.Entry<String, Integer> entry : jcache()) {
            assert entry != null;
            assert entry.getKey() != null;
            assert entry.getKey().contains(KEY_PREFIX);
            assert entry.getValue() != null;
            assert entry.getValue() >= 0 && entry.getValue() < entryCount();
            assert entry.getValue() != null;
            assert entry.getValue() >= 0 && entry.getValue() < entryCount();

            cnt++;
        }

        assert cnt == entryCount();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntrySetIteratorMultithreaded() throws Exception {
        for (int i = 0; i < gridCount(); i++)
            jcache(i).removeAll();

        final IgniteInternalFuture<?> putFut = GridTestUtils.runMultiThreadedAsync(new CAX() {
            @Override public void applyx() {
                for (int i = 0; i < entryCount(); i++)
                    jcache().put(KEY_PREFIX + i, i);
            }
        }, 1, "put-thread");

        GridTestUtils.runMultiThreaded(new CA() {
            @Override public void apply() {
                while (!putFut.isDone()) {
                    for (Cache.Entry<String, Integer> entry : jcache()) {
                        assert entry != null;
                        assert entry.getKey() != null;
                        assert entry.getKey().contains(KEY_PREFIX);
                    }
                }
            }
        }, 3, "iterator-thread");
    }

}
