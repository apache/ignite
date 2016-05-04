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

package org.apache.ignite.internal.processors.database;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.MetaStore;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.util.GridStripedLock;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.X;
import org.jsr166.ConcurrentHashMap8;

/**
 * Test with reuse.
 */
public class BPlusTreeReuseSelfTest extends BPlusTreeSelfTest {
    /** {@inheritDoc} */
    @Override protected ReuseList createReuseList(int cacheId, PageMemory pageMem, int segments, MetaStore metaStore)
        throws IgniteCheckedException {
        return new ReuseList(cacheId, pageMem, segments, metaStore);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTestRandomPutRemoveMultithreaded_1_50_0() throws Exception {
        doTestRandomPutRemoveMultithreaded(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTestRandomPutRemoveMultithreaded_1_50_1() throws Exception {
        doTestRandomPutRemoveMultithreaded(true);
    }

    /**
     * @param canGetRow Can get row from inner page.
     * @throws Exception If failed.
     */
    private void doTestRandomPutRemoveMultithreaded(boolean canGetRow) throws Exception {
        final TestTree tree = createTestTree(canGetRow);

        final Map<Long,Long> map = new ConcurrentHashMap8<>();

        final int loops = 1000_000;

        final GridStripedLock lock = new GridStripedLock(64);

        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0 ; i < loops; i++) {
                    Long x = (long)tree.randomInt(CNT);

                    boolean put = tree.randomInt(2) == 0;

                    if (i % 100_000 == 0)
                        X.println(" --> " + (put ? "put " : "rmv ") + i + "  " + x);

                    Lock l = lock.getLock(x.longValue());

                    l.lock();

                    try {
                        if (put)
                            assertEquals(map.put(x, x), tree.put(x));
                        else {
                            if (map.remove(x) != null)
                                assertEquals(x, tree.remove(x));
                            assertNull(tree.remove(x));
                        }
                    }
                    finally {
                        l.unlock();
                    }
                }

                return null;
            }
        }, 10);

        GridCursor<Long> cursor = tree.find(null, null);

        while (cursor.next()) {
            Long x = cursor.get();

            assert x != null;

            assertEquals(map.get(x), x);
        }

        assertEquals(map.size(), tree.size());
    }
}
