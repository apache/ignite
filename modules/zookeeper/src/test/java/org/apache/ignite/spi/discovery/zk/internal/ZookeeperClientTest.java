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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.test.TestingCluster;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpiAbstractTestSuite;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 *
 */
public class ZookeeperClientTest extends GridCommonAbstractTest {
    /** */
    private static final int SES_TIMEOUT = 60_000;

    /** */
    private TestingCluster zkCluster;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        closeZK();

        super.afterTest();
    }

    /**
     * @param sesTimeout Session timeout.
     * @return Client.
     * @throws Exception If failed.
     */
    private ZookeeperClient createClient(int sesTimeout) throws Exception {
        return new ZookeeperClient(log, zkCluster.getConnectString(), sesTimeout, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSaveLargeValue() throws Exception {
        startZK(1);

        final ZookeeperClient client = createClient(SES_TIMEOUT);

        byte[] data = new byte[1024 * 1024];

        String basePath = "/ignite";

        assertTrue(client.needSplitNodeData(basePath, data, 2));

        List<byte[]> parts = client.splitNodeData(basePath, data, 2);

        assertTrue(parts.size() > 1);

        ZooKeeper zk = client.zk();

        for (int i = 0; i < parts.size(); i++) {
            byte[] part = parts.get(i);

            assertTrue(part.length > 0);

            String path0 = basePath + ":" + i;

            zk.create(path0, part, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClose() throws Exception {
        startZK(1);

        final ZookeeperClient client = createClient(SES_TIMEOUT);

        client.createIfNeeded("/apacheIgnite1", null, CreateMode.PERSISTENT);

        client.zk().close();

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                client.createIfNeeded("/apacheIgnite2", null, CreateMode.PERSISTENT);

                return null;
            }
        }, ZookeeperClientFailedException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateAll() throws Exception {
        startZK(1);

        ZookeeperClient client = createClient(SES_TIMEOUT);

        client.createIfNeeded("/apacheIgnite", null, CreateMode.PERSISTENT);

        List<String> paths = new ArrayList<>();

        paths.add("/apacheIgnite/1");
        paths.add("/apacheIgnite/2");
        paths.add("/apacheIgnite/3");

        client.createAll(paths, CreateMode.PERSISTENT);

        assertEquals(3, client.getChildren("/apacheIgnite").size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateAllRequestOverflow() throws Exception {
        startZK(1);

        ZookeeperClient client = createClient(SES_TIMEOUT);

        client.createIfNeeded("/apacheIgnite", null, CreateMode.PERSISTENT);

        int cnt = 20_000;

        List<String> paths = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++)
            paths.add("/apacheIgnite/" + i);

        client.createAll(paths, CreateMode.PERSISTENT);

        assertEquals(cnt, client.getChildren("/apacheIgnite").size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateAllNodeExists() throws Exception {
        startZK(1);

        ZookeeperClient client = createClient(SES_TIMEOUT);

        client.createIfNeeded("/apacheIgnite", null, CreateMode.PERSISTENT);

        client.createIfNeeded("/apacheIgnite/1", null, CreateMode.PERSISTENT);

        List<String> paths = new ArrayList<>();

        paths.add("/apacheIgnite/1");
        paths.add("/apacheIgnite/2");
        paths.add("/apacheIgnite/3");

        client.createAll(paths, CreateMode.PERSISTENT);

        assertEquals(3, client.getChildren("/apacheIgnite").size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeleteAll() throws Exception {
        startZK(1);

        ZookeeperClient client = createClient(SES_TIMEOUT);

        client.createIfNeeded("/apacheIgnite", null, CreateMode.PERSISTENT);
        client.createIfNeeded("/apacheIgnite/1", null, CreateMode.PERSISTENT);
        client.createIfNeeded("/apacheIgnite/2", null, CreateMode.PERSISTENT);

        client.deleteAll("/apacheIgnite", Arrays.asList("1", "2"), -1);

        assertTrue(client.getChildren("/apacheIgnite").isEmpty());

        client.createIfNeeded("/apacheIgnite/1", null, CreateMode.PERSISTENT);
        client.deleteAll("/apacheIgnite", Collections.singletonList("1"), -1);

        assertTrue(client.getChildren("/apacheIgnite").isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeleteAllRequestOverflow() throws Exception {
        startZK(1);

        ZookeeperClient client = createClient(SES_TIMEOUT);

        client.createIfNeeded("/apacheIgnite", null, CreateMode.PERSISTENT);

        int cnt = 30_000;

        List<String> paths = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++)
            paths.add("/apacheIgnite/" + i);

        client.createAll(paths, CreateMode.PERSISTENT);

        assertEquals(cnt, client.getChildren("/apacheIgnite").size());

        List<String> subPaths = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++)
            subPaths.add(String.valueOf(i));

        client.deleteAll("/apacheIgnite", subPaths, -1);

        assertTrue(client.getChildren("/apacheIgnite").isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeleteAllNoNode() throws Exception {
        startZK(1);

        ZookeeperClient client = createClient(SES_TIMEOUT);

        client.createIfNeeded("/apacheIgnite", null, CreateMode.PERSISTENT);
        client.createIfNeeded("/apacheIgnite/1", null, CreateMode.PERSISTENT);
        client.createIfNeeded("/apacheIgnite/2", null, CreateMode.PERSISTENT);

        client.deleteAll("/apacheIgnite", Arrays.asList("1", "2", "3"), -1);

        assertTrue(client.getChildren("/apacheIgnite").isEmpty());
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnectionLoss1() throws Exception {
        ZookeeperClient client = new ZookeeperClient(log, "localhost:2200", 3000, null);

        try {
            client.createIfNeeded("/apacheIgnite", null, CreateMode.PERSISTENT);

            fail();
        }
        catch (ZookeeperClientFailedException e) {
            info("Expected error: " + e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnectionLoss2() throws Exception {
        startZK(1);

        ZookeeperClient client = createClient(3000);

        client.createIfNeeded("/apacheIgnite1", null, CreateMode.PERSISTENT);

        closeZK();

        try {
            client.createIfNeeded("/apacheIgnite2", null, CreateMode.PERSISTENT);

            fail();
        }
        catch (ZookeeperClientFailedException e) {
            info("Expected error: " + e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnectionLoss3() throws Exception {
        startZK(1);

        CallbackFuture cb = new CallbackFuture();

        ZookeeperClient client = new ZookeeperClient(log, zkCluster.getConnectString(), 3000, cb);

        client.createIfNeeded("/apacheIgnite1", null, CreateMode.PERSISTENT);

        closeZK();

        final AtomicBoolean res = new AtomicBoolean();

        client.getChildrenAsync("/apacheIgnite1", null, new AsyncCallback.Children2Callback() {
            @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                if (rc == 0)
                    res.set(true);
            }
        });

        cb.get(60_000);

        assertFalse(res.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnectionLoss4() throws Exception {
        startZK(1);

        CallbackFuture cb = new CallbackFuture();

        final ZookeeperClient client = new ZookeeperClient(log, zkCluster.getConnectString(), 3000, cb);

        client.createIfNeeded("/apacheIgnite1", null, CreateMode.PERSISTENT);

        final CountDownLatch l = new CountDownLatch(1);

        client.getChildrenAsync("/apacheIgnite1", null, new AsyncCallback.Children2Callback() {
            @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                closeZK();

                try {
                    client.createIfNeeded("/apacheIgnite2", null, CreateMode.PERSISTENT);
                }
                catch (ZookeeperClientFailedException e) {
                    info("Expected error: " + e);

                    l.countDown();
                }
                catch (Exception e) {
                    fail("Unexpected error: " + e);
                }
            }
        });

        assertTrue(l.await(10, TimeUnit.SECONDS));

        cb.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnect1() throws Exception {
        startZK(1);

        ZookeeperClient client = createClient(SES_TIMEOUT);

        client.createIfNeeded("/apacheIgnite1", null, CreateMode.PERSISTENT);

        zkCluster.getServers().get(0).stop();

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                U.sleep(2000);

                info("Restart zookeeper server");

                zkCluster.getServers().get(0).restart();

                info("Zookeeper server restarted");

                return null;
            }
        }, "start-zk");

        client.createIfNeeded("/apacheIgnite2", null, CreateMode.PERSISTENT);

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnect1_Callback() throws Exception {
        startZK(1);

        ZookeeperClient client = createClient(SES_TIMEOUT);

        client.createIfNeeded("/apacheIgnite1", null, CreateMode.PERSISTENT);

        zkCluster.getServers().get(0).stop();

        final CountDownLatch l = new CountDownLatch(1);

        client.getChildrenAsync("/apacheIgnite1", null, new AsyncCallback.Children2Callback() {
            @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                info("Callback: " + rc);

                if (rc == 0)
                    l.countDown();
            }
        });

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                U.sleep(2000);

                info("Restart zookeeper server");

                zkCluster.getServers().get(0).restart();

                info("Zookeeper server restarted");

                return null;
            }
        }, "start-zk");

        assertTrue(l.await(10, TimeUnit.SECONDS));

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnect1_InCallback() throws Exception {
        startZK(1);

        final ZookeeperClient client = createClient(SES_TIMEOUT);

        client.createIfNeeded("/apacheIgnite1", null, CreateMode.PERSISTENT);

        final CountDownLatch l = new CountDownLatch(1);

        client.getChildrenAsync("/apacheIgnite1", null, new AsyncCallback.Children2Callback() {
            @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                try {
                    zkCluster.getServers().get(0).stop();

                    IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            U.sleep(2000);

                            info("Restart zookeeper server");

                            zkCluster.getServers().get(0).restart();

                            info("Zookeeper server restarted");

                            return null;
                        }
                    }, "start-zk");

                    client.createIfNeeded("/apacheIgnite2", null, CreateMode.PERSISTENT);

                    l.countDown();

                    fut.get();
                }
                catch (Exception e) {
                    fail("Unexpected error: " + e);
                }
            }
        });

        assertTrue(l.await(10, TimeUnit.SECONDS));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnect2() throws Exception {
        startZK(1);

        ZookeeperClient client = createClient(SES_TIMEOUT);

        client.createIfNeeded("/apacheIgnite1", null, CreateMode.PERSISTENT);

        zkCluster.getServers().get(0).restart();

        client.createIfNeeded("/apacheIgnite2", null, CreateMode.PERSISTENT);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnect3() throws Exception {
        startZK(3);

        ZookeeperClient client = createClient(SES_TIMEOUT);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 30; i++) {
            info("Iteration: " + i);

            int idx = rnd.nextInt(3);

            zkCluster.getServers().get(idx).restart();

            doSleep(rnd.nextLong(100) + 1);

            client.createIfNeeded("/apacheIgnite" + i, null, CreateMode.PERSISTENT);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnect4() throws Exception {
        startZK(3);

        ZookeeperClient client = new ZookeeperClient(log,
            zkCluster.getServers().get(2).getInstanceSpec().getConnectString(),
            60_000,
            null);

        client.createIfNeeded("/apacheIgnite1", null, CreateMode.PERSISTENT);

        zkCluster.getServers().get(0).stop();
        zkCluster.getServers().get(1).stop();

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                U.sleep(2000);

                info("Restart zookeeper server");

                zkCluster.getServers().get(0).restart();

                info("Zookeeper server restarted");

                return null;
            }
        }, "start-zk");

        client.createIfNeeded("/apacheIgnite2", null, CreateMode.PERSISTENT);

        fut.get();
    }

    /**
     * @param instances Number of servers in ZK ensemble.
     * @throws Exception If failed.
     */
    private void startZK(int instances) throws Exception {
        assert zkCluster == null;

        zkCluster = ZookeeperDiscoverySpiAbstractTestSuite.createTestingCluster(instances);

        zkCluster.start();
    }

    /**
     *
     */
    private void closeZK() {
        if (zkCluster != null) {
            try {
                zkCluster.close();
            }
            catch (Exception e) {
                U.error(log, "Failed to stop Zookeeper client: " + e, e);
            }

            zkCluster = null;
        }
    }

    /**
     *
     */
    private static class CallbackFuture extends GridFutureAdapter<Void> implements IgniteRunnable {
        /** {@inheritDoc} */
        @Override public void run() {
            onDone();
        }
    }
}
