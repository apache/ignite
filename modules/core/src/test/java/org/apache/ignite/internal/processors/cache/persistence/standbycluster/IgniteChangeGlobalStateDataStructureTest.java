/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.standbycluster;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
public class IgniteChangeGlobalStateDataStructureTest extends IgniteChangeGlobalStateAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeActivateAndActivateAtomicLong() throws Exception {
        String lName = "myLong";

        Ignite ig1 = primary(0);
        Ignite ig2 = primary(1);
        Ignite ig3 = primary(2);

        IgniteAtomicLong lexp = ig1.atomicLong(lName, 100, true);

        lexp.incrementAndGet();
        lexp.incrementAndGet();
        lexp.incrementAndGet();

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(ig3.active());

        ig2.active(false);

        IgniteEx ex1 = (IgniteEx)ig1;
        IgniteEx ex2 = (IgniteEx)ig2;
        IgniteEx ex3 = (IgniteEx)ig3;

        GridCacheProcessor cache1 = ex1.context().cache();
        GridCacheProcessor cache2 = ex2.context().cache();
        GridCacheProcessor cache3 = ex3.context().cache();

        assertTrue(F.isEmpty(cache1.jcaches()));
        assertTrue(F.isEmpty(cache2.jcaches()));
        assertTrue(F.isEmpty(cache3.jcaches()));

        assertTrue(!ig1.active());
        assertTrue(!ig2.active());
        assertTrue(!ig3.active());

        ig3.active(true);

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(ig3.active());

        IgniteAtomicLong lact1 = ig1.atomicLong(lName, 0, false);
        IgniteAtomicLong lact2 = ig2.atomicLong(lName, 0, false);
        IgniteAtomicLong lact3 = ig3.atomicLong(lName, 0, false);

        assertEquals(103, lact1.get());
        assertEquals(103, lact2.get());
        assertEquals(103, lact3.get());

        lact1.incrementAndGet();

        assertEquals(104, lact1.get());
        assertEquals(104, lact2.get());
        assertEquals(104, lact3.get());

        lact2.incrementAndGet();

        assertEquals(105, lact1.get());
        assertEquals(105, lact2.get());
        assertEquals(105, lact3.get());

        lact3.incrementAndGet();

        assertEquals(106, lact3.get());
        assertEquals(106, lact3.get());
        assertEquals(106, lact3.get());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeActivateAndActivateCountDownLatch() throws Exception {
        final AtomicInteger cnt = new AtomicInteger();

        String latchName = "myLatch";

        Ignite ig1 = primary(0);
        Ignite ig2 = primary(1);
        Ignite ig3 = primary(2);

        IgniteCountDownLatch latchExp1 = ig1.countDownLatch(latchName, 5, false, true);

        latchExp1.countDown();

        assertEquals(4, latchExp1.count());

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(ig3.active());

        ig2.active(false);

        IgniteEx ex1 = (IgniteEx)ig1;
        IgniteEx ex2 = (IgniteEx)ig2;
        IgniteEx ex3 = (IgniteEx)ig3;

        GridCacheProcessor cache1 = ex1.context().cache();
        GridCacheProcessor cache2 = ex2.context().cache();
        GridCacheProcessor cache3 = ex3.context().cache();

        assertTrue(F.isEmpty(cache1.jcaches()));
        assertTrue(F.isEmpty(cache2.jcaches()));
        assertTrue(F.isEmpty(cache3.jcaches()));

        assertTrue(!ig1.active());
        assertTrue(!ig2.active());
        assertTrue(!ig3.active());

        ig3.active(true);

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(ig3.active());

        final IgniteCountDownLatch latchAct1 = ig1.countDownLatch(latchName, 0, false, false);
        final IgniteCountDownLatch latchAct2 = ig2.countDownLatch(latchName, 0, false, false);
        final IgniteCountDownLatch latchAct3 = ig3.countDownLatch(latchName, 0, false, false);

        runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                latchAct1.await();
                cnt.incrementAndGet();
                return null;
            }
        });

        runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                latchAct2.await();
                cnt.incrementAndGet();
                return null;
            }
        });

        runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                latchAct3.await();
                cnt.incrementAndGet();
                return null;
            }
        });

        assertEquals(4, latchAct1.count());
        assertEquals(4, latchAct2.count());
        assertEquals(4, latchAct3.count());

        latchAct1.countDown();
        latchAct2.countDown();
        latchAct3.countDown();

        assertEquals(1, latchAct1.count());
        assertEquals(1, latchAct2.count());
        assertEquals(1, latchAct3.count());

        latchAct1.countDown();

        U.sleep(3000);

        assertEquals(3, cnt.get());
    }

    /**
     *
     */
    @Test
    public void testDeActivateAndActivateAtomicSequence() {
        String seqName = "mySeq";

        Ignite ig1 = primary(0);
        Ignite ig2 = primary(1);
        Ignite ig3 = primary(2);

        IgniteAtomicSequence seqExp1 = ig1.atomicSequence(seqName, 0, true);
        IgniteAtomicSequence seqExp2 = ig2.atomicSequence(seqName, 0, false);
        IgniteAtomicSequence seqExp3 = ig3.atomicSequence(seqName, 0, false);

        assertEquals(0, seqExp1.get());
        assertEquals(1000, seqExp2.get());
        assertEquals(2000, seqExp3.get());

        seqExp1.incrementAndGet();
        seqExp2.incrementAndGet();
        seqExp3.incrementAndGet();

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(ig3.active());

        ig2.active(false);

        IgniteEx ex1 = (IgniteEx)ig1;
        IgniteEx ex2 = (IgniteEx)ig2;
        IgniteEx ex3 = (IgniteEx)ig3;

        GridCacheProcessor cache1 = ex1.context().cache();
        GridCacheProcessor cache2 = ex2.context().cache();
        GridCacheProcessor cache3 = ex3.context().cache();

        assertTrue(F.isEmpty(cache1.jcaches()));
        assertTrue(F.isEmpty(cache2.jcaches()));
        assertTrue(F.isEmpty(cache3.jcaches()));

        assertTrue(!ig1.active());
        assertTrue(!ig2.active());
        assertTrue(!ig3.active());

        ig3.active(true);

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(ig3.active());

        IgniteAtomicSequence seqAct1 = ig1.atomicSequence(seqName, 0, false);
        IgniteAtomicSequence seqAct2 = ig2.atomicSequence(seqName, 0, false);
        IgniteAtomicSequence seqAct3 = ig3.atomicSequence(seqName, 0, false);

        assertEquals(1, seqAct1.get());
        assertEquals(1001, seqAct2.get());
        assertEquals(2001, seqAct3.get());

        seqAct1.incrementAndGet();

        assertEquals(2, seqAct1.get());
        assertEquals(1001, seqAct2.get());
        assertEquals(2001, seqAct3.get());

        seqAct2.incrementAndGet();

        assertEquals(2, seqAct1.get());
        assertEquals(1002, seqAct2.get());
        assertEquals(2001, seqAct3.get());

        seqAct3.incrementAndGet();

        assertEquals(2, seqAct1.get());
        assertEquals(1002, seqAct2.get());
        assertEquals(2002, seqAct3.get());
    }
}
