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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests for methods that run job locally with multiple arguments.
 */
public class GridProjectionLocalJobMultipleArgumentsSelfTest extends GridCommonAbstractTest {
    /** */
    private static Collection<Object> ids;

    /** */
    private static AtomicInteger res;

    /**
     * Starts grid.
     */
    public GridProjectionLocalJobMultipleArgumentsSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cache = defaultCacheConfiguration();

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);

        cfg.setCacheConfiguration(cache);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ids = new GridConcurrentHashSet<>();
        res = new AtomicInteger();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityCall() throws Exception {
        Collection<Integer> res = new ArrayList<>();

        for (int i : F.asList(1, 2, 3)) {
            res.add(grid().compute().affinityCall(DEFAULT_CACHE_NAME, i, new IgniteCallable<Integer>() {
                @Override public Integer call() {
                    ids.add(this);

                    return 10;
                }
            }));
        }

        assertEquals(30, F.sumInt(res));
        assertEquals(3, ids.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityRun() throws Exception {
        for (int i : F.asList(1, 2, 3)) {
            grid().compute().affinityRun(DEFAULT_CACHE_NAME, i, new IgniteRunnable() {
                @Override public void run() {
                    ids.add(this);

                    res.addAndGet(10);
                }
            });
        }

        assertEquals(30, res.get());
        assertEquals(3, ids.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCall() throws Exception {
        Collection<Integer> res = grid().compute().apply(new C1<Integer, Integer>() {
            @Override public Integer apply(Integer arg) {
                ids.add(this);

                return 10 + arg;
            }
        }, F.asList(1, 2, 3));

        assertEquals(36, F.sumInt(res));
        assertEquals(3, ids.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCallWithProducer() throws Exception {
        Collection<Integer> args = Arrays.asList(1, 2, 3);

        Collection<Integer> res = grid().compute().apply(new C1<Integer, Integer>() {
            @Override public Integer apply(Integer arg) {
                ids.add(this);

                return 10 + arg;
            }
        }, args);

        assertEquals(36, F.sumInt(res));
        assertEquals(3, ids.size());
    }
}
