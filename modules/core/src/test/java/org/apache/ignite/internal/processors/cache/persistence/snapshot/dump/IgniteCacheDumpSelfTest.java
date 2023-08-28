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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.junit.Test;

import static org.junit.Assume.assumeTrue;

/** */
public class IgniteCacheDumpSelfTest extends AbstractCacheDumpTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setSnapshotThreadPoolSize(1);
    }

    /** */
    @Test
    public void testCacheDump() throws Exception {
        IgniteEx ign = startGridAndFillCaches();

        createDump(ign);

        checkDump(ign);
    }

    /** */
    @Test
    public void testWithConcurrentInsert() throws Exception {
        doTestWithOperations(cache -> {
            for (int i = KEYS_CNT; i < KEYS_CNT + 3; i++) {
                assertFalse(cache.containsKey(i));

                cache.put(i, i);
            }
        });
    }

    /** */
    @Test
    public void testWithConcurrentUpdate() throws Exception {
        doTestWithOperations(cache -> {
            for (int i = 0; i < 3; i++) {
                assertTrue(cache.containsKey(i));

                cache.put(i, i + 1);
            }
        });
    }

    /** */
    @Test
    public void testWithConcurrentRemove() throws Exception {
        doTestWithOperations(cache -> {
            for (int i = 0; i < 3; i++) {
                assertTrue(cache.containsKey(i));

                cache.remove(i);
            }
        });
    }

    /** */
    private void doTestWithOperations(Consumer<IgniteCache<Object, Object>> op) throws Exception {
        assumeTrue(nodes == 1);

        IgniteEx ign = startGridAndFillCaches();

        CountDownLatch latch = new CountDownLatch(1);

        IgniteThreadPoolExecutor snpExec = (IgniteThreadPoolExecutor)ign.context().pools().getSnapshotExecutorService();

        snpExec.submit(() -> {
            try {
                latch.await();
            }
            catch (InterruptedException e) {
                throw new IgniteException(e);
            }
        });

        IgniteInternalFuture<Object> dumpFut = GridTestUtils.runAsync(() -> createDump(ign));

        GridTestUtils.waitForCondition(() -> snpExec.getQueue().size() > 1, 1_000);

        op.accept(ign.cache(DEFAULT_CACHE_NAME));

        latch.countDown();

        dumpFut.get();

        checkDump(ign);
    }
}
