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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
public class IgniteChangeGlobalStateTest extends IgniteChangeGlobalStateAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-9004");

        super.beforeTest();
    }

    /**
     * @throws Exception if fail.
     */
    public void testStopPrimaryAndActivateFromServerNode() throws Exception {
        Ignite ig1P = primary(0);
        Ignite ig2P = primary(1);
        Ignite ig3P = primary(2);

        Ignite ig1B = backUp(0);
        Ignite ig2B = backUp(1);
        Ignite ig3B = backUp(2);

        assertTrue(ig1P.active());
        assertTrue(ig2P.active());
        assertTrue(ig3P.active());

        assertTrue(!ig1B.active());
        assertTrue(!ig2B.active());
        assertTrue(!ig3B.active());

        stopAllPrimary();

        ig2B.active(true);

        assertTrue(ig1B.active());
        assertTrue(ig2B.active());
        assertTrue(ig3B.active());
    }

    /**
     * @throws Exception if fail.
     */
    public void testStopPrimaryAndActivateFromClientNode() throws Exception {
        Ignite ig1P = primary(0);
        Ignite ig2P = primary(1);
        Ignite ig3P = primary(2);

        Ignite ig1B = backUp(0);
        Ignite ig2B = backUp(1);
        Ignite ig3B = backUp(2);

        Ignite ig1C = backUpClient(0);
        Ignite ig2C = backUpClient(1);
        Ignite ig3C = backUpClient(2);

        assertTrue(ig1P.active());
        assertTrue(ig2P.active());
        assertTrue(ig3P.active());

        assertTrue(!ig1B.active());
        assertTrue(!ig2B.active());
        assertTrue(!ig3B.active());

        assertTrue(!ig1C.active());
        assertTrue(!ig2C.active());
        assertTrue(!ig3C.active());

        stopAllPrimary();

        ig2B.active(true);

        assertTrue(ig1B.active());
        assertTrue(ig2B.active());
        assertTrue(ig3B.active());

        assertTrue(ig1C.active());
        assertTrue(ig2C.active());
        assertTrue(ig3C.active());
    }

    /**
     * @throws Exception if fail.
     */
    public void testConcurrentActivateFromClientNodeAndServerNode() throws Exception {
        final Ignite ig1B = backUp(0);
        final Ignite ig2B = backUp(1);
        final Ignite ig3B = backUp(2);

        final Ignite ig1C = backUpClient(0);
        final Ignite ig2C = backUpClient(1);
        final Ignite ig3C = backUpClient(2);

        assertTrue(!ig1B.active());
        assertTrue(!ig2B.active());
        assertTrue(!ig3B.active());

        stopAllPrimary();

        final CyclicBarrier barrier = new CyclicBarrier(backUpNodes() + backUpClientNodes());

        IgniteInternalFuture<Void> f1 = runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                barrier.await();

                ig1B.active(true);

                return null;
            }
        });

        IgniteInternalFuture<?> f2 = runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                barrier.await();

                ig2B.active(true);

                return null;
            }
        });

        IgniteInternalFuture<?> f3 = runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                barrier.await();

                ig3B.active(true);

                return null;
            }
        });

        IgniteInternalFuture<?> f4 = runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                barrier.await();

                ig1C.active(true);

                return null;
            }
        });

        IgniteInternalFuture<?> f5 = runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                barrier.await();

                ig2C.active(true);

                return null;
            }
        });

        IgniteInternalFuture<?> f6 = runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                barrier.await();

                ig3C.active(true);

                return null;
            }
        });

        f1.get();
        f2.get();
        f3.get();

        f4.get();
        f5.get();
        f6.get();

        assertTrue(ig1B.active());
        assertTrue(ig2B.active());
        assertTrue(ig3B.active());

        assertTrue(ig1C.active());
        assertTrue(ig2C.active());
        assertTrue(ig3C.active());
    }

    /**
     * @throws Exception if fail.
     */
    public void testConcurrentActivateFromServerNode() throws Exception {
        final Ignite ig1B = backUp(0);
        final Ignite ig2B = backUp(1);
        final Ignite ig3B = backUp(2);

        assertTrue(!ig1B.active());
        assertTrue(!ig2B.active());
        assertTrue(!ig3B.active());

        stopAllPrimary();

        final CyclicBarrier barrier = new CyclicBarrier(3);

        IgniteInternalFuture<?> act1 = runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                barrier.await();

                ig1B.active(true);

                return null;
            }
        });

        IgniteInternalFuture<?> act2 = runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                barrier.await();

                ig2B.active(true);

                return null;
            }
        });

        IgniteInternalFuture<?> act3 = runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                barrier.await();

                ig3B.active(true);

                return null;
            }
        });

        act1.get();
        act2.get();
        act3.get();

        assertTrue(ig1B.active());
        assertTrue(ig2B.active());
        assertTrue(ig3B.active());
    }

    /**
     * @throws Exception If failed.
     */
    public void testActiveAndInActiveAtTheSameTimeCluster() throws Exception {
        Ignite ig1P = primary(0);
        Ignite ig2P = primary(1);
        Ignite ig3P = primary(2);

        Ignite ig1B = backUp(0);
        Ignite ig2B = backUp(1);
        Ignite ig3B = backUp(2);

        assertTrue(ig1P.cluster().nodes().size() == 6);
        assertTrue(ig2P.cluster().nodes().size() == 6);
        assertTrue(ig3P.cluster().nodes().size() == 6);

        List<ClusterNode> primaryNodes = Arrays.asList(
            ig1P.cluster().localNode(), ig2P.cluster().localNode(), ig3P.cluster().localNode()
        );

        List<ClusterNode> backUpNodes = Arrays.asList(
            ig1B.cluster().localNode(), ig3B.cluster().localNode(), ig3B.cluster().localNode()
        );

        assertTrue(!ig1P.cluster().forRemotes().nodes().containsAll(backUpNodes));
        assertTrue(!ig2P.cluster().forRemotes().nodes().containsAll(backUpNodes));
        assertTrue(!ig3P.cluster().forRemotes().nodes().containsAll(backUpNodes));

        assertTrue(ig1B.cluster().nodes().size() == 6);
        assertTrue(ig2B.cluster().nodes().size() == 6);
        assertTrue(ig3B.cluster().nodes().size() == 6);

        assertTrue(!ig1B.cluster().forRemotes().nodes().containsAll(primaryNodes));
        assertTrue(!ig2B.cluster().forRemotes().nodes().containsAll(primaryNodes));
        assertTrue(!ig3B.cluster().forRemotes().nodes().containsAll(primaryNodes));
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateOnAlreadyActivatedCluster() throws Exception {
        Ignite ig1P = primary(0);
        Ignite ig2P = primary(1);
        Ignite ig3P = primary(2);

        Ignite ig1B = backUp(0);
        Ignite ig2B = backUp(1);
        Ignite ig3B = backUp(2);

        Ignite ig1C = backUpClient(0);
        Ignite ig2C = backUpClient(1);
        Ignite ig3C = backUpClient(2);

        assertTrue(ig1P.active());
        assertTrue(ig2P.active());
        assertTrue(ig3P.active());

        assertTrue(!ig1B.active());
        assertTrue(!ig2B.active());
        assertTrue(!ig3B.active());

        assertTrue(!ig1C.active());
        assertTrue(!ig2C.active());
        assertTrue(!ig3C.active());

        stopAllPrimary();

        ig2B.active(true);

        assertTrue(ig1B.active());
        assertTrue(ig2B.active());
        assertTrue(ig3B.active());

        assertTrue(ig1C.active());
        assertTrue(ig2C.active());
        assertTrue(ig3C.active());

        ig1B.active(true);
        ig2B.active(true);
        ig3B.active(true);

        ig1C.active(true);
        ig2C.active(true);
        ig3C.active(true);

        assertTrue(ig1B.active());
        assertTrue(ig2B.active());
        assertTrue(ig3B.active());

        assertTrue(ig1C.active());
        assertTrue(ig2C.active());
        assertTrue(ig3C.active());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTryUseCacheInActiveCluster() throws Exception {
        Ignite ig1B = backUp(0);
        Ignite ig2B = backUp(1);
        Ignite ig3B = backUp(2);

        Ignite ig1C = backUpClient(0);
        Ignite ig2C = backUpClient(1);
        Ignite ig3C = backUpClient(2);

        assertTrue(!ig1B.active());
        assertTrue(!ig2B.active());
        assertTrue(!ig3B.active());

        assertTrue(!ig1C.active());
        assertTrue(!ig2C.active());
        assertTrue(!ig3C.active());

       checkExceptionTryUseCache(ig1B);
       checkExceptionTryUseCache(ig2B);
       checkExceptionTryUseCache(ig3B);

       checkExceptionTryUseCache(ig1C);
       checkExceptionTryUseCache(ig2C);
       checkExceptionTryUseCache(ig3C);
    }

    /**
     * @param ig Ig.
     */
    private void checkExceptionTryUseCache(final Ignite ig) {
        assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteCache cache = ig.cache("cache");

                return null;
            }
        }, IgniteException.class, "Can not perform the operation because the cluster is inactive.");
    }

    /**
     * @throws Exception If failed.
     */
    public void testTryUseServiceInActiveCluster() throws Exception {
        Ignite ig1B = backUp(0);
        Ignite ig2B = backUp(1);
        Ignite ig3B = backUp(2);

        Ignite ig1C = backUpClient(0);
        Ignite ig2C = backUpClient(1);
        Ignite ig3C = backUpClient(2);

        assertTrue(!ig1B.active());
        assertTrue(!ig2B.active());
        assertTrue(!ig3B.active());

        assertTrue(!ig1C.active());
        assertTrue(!ig2C.active());
        assertTrue(!ig3C.active());

        checkExceptionTryUseService(ig1B);
        checkExceptionTryUseService(ig2B);
        checkExceptionTryUseService(ig3B);

        checkExceptionTryUseService(ig1C);
        checkExceptionTryUseService(ig2C);
        checkExceptionTryUseService(ig3C);
    }

    /**
     * @param ig Node to check.
     */
    private void checkExceptionTryUseService(final Ignite ig) {
        assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteServices srvs = ig.services();

                return null;
            }
        }, IgniteException.class, "Can not perform the operation because the cluster is inactive.");
    }

    /**
     * @throws Exception If failed.
     */
    public void testTryUseDataStructureInActiveCluster() throws Exception {
        Ignite ig1B = backUp(0);
        Ignite ig2B = backUp(1);
        Ignite ig3B = backUp(2);

        Ignite ig1C = backUpClient(0);
        Ignite ig2C = backUpClient(1);
        Ignite ig3C = backUpClient(2);

        assertTrue(!ig1B.active());
        assertTrue(!ig2B.active());
        assertTrue(!ig3B.active());

        assertTrue(!ig1C.active());
        assertTrue(!ig2C.active());
        assertTrue(!ig3C.active());

        checkExceptionTryUseDataStructure(ig1B);
        checkExceptionTryUseDataStructure(ig2B);
        checkExceptionTryUseDataStructure(ig3B);

        checkExceptionTryUseDataStructure(ig1C);
        checkExceptionTryUseDataStructure(ig2C);
        checkExceptionTryUseDataStructure(ig3C);
    }

    /**
     * @param ig Node.
     */
    private void checkExceptionTryUseDataStructure(final Ignite ig){
        assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteAtomicSequence seq = ig.atomicSequence("seq", 0, true);

                return null;
            }
        }, IgniteException.class, "Can not perform the operation because the cluster is inactive.");
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailGetLock() throws Exception {
        Ignite ig1P = primary(0);
        Ignite ig2P = primary(1);
        Ignite ig3P = primary(2);

        Ignite ig1B = backUp(0);
        Ignite ig2B = backUp(1);
        Ignite ig3B = backUp(2);

        Ignite ig1C = backUpClient(0);
        Ignite ig2C = backUpClient(1);

        final Ignite ig3C = backUpClient(2);

        assertTrue(ig1P.active());
        assertTrue(ig2P.active());
        assertTrue(ig3P.active());

        assertTrue(!ig1B.active());
        assertTrue(!ig2B.active());
        assertTrue(!ig3B.active());

        assertTrue(!ig1C.active());
        assertTrue(!ig2C.active());
        assertTrue(!ig3C.active());

        stopPrimary(0);

        boolean exc = false;

        try {
            ig3C.active(true);
        }
        catch (IgniteException e) {
            exc = true;

            log.error("stack trace from remote node", e);

            for (Throwable t : e.getSuppressed())
                assertTrue(t.getMessage().contains("can't get lock during"));
        }

        if (!exc)
            fail();

        assertTrue(!ig1B.active());
        assertTrue(!ig2B.active());
        assertTrue(!ig3B.active());

        assertTrue(!ig1C.active());
        assertTrue(!ig2C.active());
        assertTrue(!ig3C.active());
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateAfterFailGetLock() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-1094");

        Ignite ig1P = primary(0);
        Ignite ig2P = primary(1);
        Ignite ig3P = primary(2);

        Ignite ig1CP = primaryClient(0);
        Ignite ig2CP = primaryClient(1);
        Ignite ig3CP = primaryClient(2);

        Ignite ig1B = backUp(0);
        Ignite ig2B = backUp(1);
        Ignite ig3B = backUp(2);

        Ignite ig1CB = backUpClient(0);
        Ignite ig2CB = backUpClient(1);
        Ignite ig3CB = backUpClient(2);

        assertTrue(ig1P.active());
        assertTrue(ig2P.active());
        assertTrue(ig3P.active());

        assertTrue(!ig1B.active());
        assertTrue(!ig2B.active());
        assertTrue(!ig3B.active());

        assertTrue(!ig1CB.active());
        assertTrue(!ig2CB.active());
        assertTrue(!ig3CB.active());

        stopPrimary(0);

        try {
            ig3CB.active(true);

            fail("Activation should fail");
        }
        catch (IgniteException e) {
            log.error("Stack trace from remote node", e);

            for (Throwable t : e.getSuppressed())
                assertTrue(t.getMessage().contains("can't get lock during"));
        }

        assertTrue(!ig1B.active());
        assertTrue(!ig2B.active());
        assertTrue(!ig3B.active());

        assertTrue(!ig1CB.active());
        assertTrue(!ig2CB.active());
        assertTrue(!ig3CB.active());

        assertTrue(ig2P.active());
        assertTrue(ig3P.active());

        assertTrue(ig1CP.active());
        assertTrue(ig2CP.active());
        assertTrue(ig3CP.active());

        stopAllPrimary();

        ig2CB.active(true);

        assertTrue(ig1B.active());
        assertTrue(ig2B.active());
        assertTrue(ig3B.active());

        assertTrue(ig1CB.active());
        assertTrue(ig2CB.active());
        assertTrue(ig3CB.active());
    }

    /**
     * @throws Exception if fail.
     */
    public void testDeActivateFromServerNode() throws Exception {
        Ignite ig1 = primary(0);
        Ignite ig2 = primary(1);
        Ignite ig3 = primary(2);

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(ig3.active());

        ig1.active(false);

        assertTrue(!ig1.active());
        assertTrue(!ig2.active());
        assertTrue(!ig3.active());
    }

    /**
     * @throws Exception if fail.
     */
    public void testDeActivateFromClientNode() throws Exception {
        Ignite ig1 = primary(0);
        Ignite ig2 = primary(1);
        Ignite ig3 = primary(2);

        Ignite ig1C = primaryClient(0);
        Ignite ig2C = primaryClient(1);
        Ignite ig3C = primaryClient(2);

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(ig3.active());

        ig1C.active(false);

        assertTrue(!ig1.active());
        assertTrue(!ig2.active());
        assertTrue(!ig3.active());

        assertTrue(!ig1C.active());
        assertTrue(!ig2C.active());
        assertTrue(!ig3C.active());
    }

    /**
     * @throws Exception if fail.
     */
    public void testDeActivateCheckCacheDestroy() throws Exception {
        String chName = "myCache";

        Ignite ig1 = primary(0);
        Ignite ig2 = primary(1);
        Ignite ig3 = primary(2);

        ig1.getOrCreateCache(chName);

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(ig3.active());

        ig1.active(false);

        assertTrue(!ig1.active());
        assertTrue(!ig2.active());
        assertTrue(!ig3.active());

        IgniteEx ex = (IgniteEx)ig1;

        GridCacheProcessor cache = ex.context().cache();

        assertTrue(F.isEmpty(cache.jcaches()));
    }
}
