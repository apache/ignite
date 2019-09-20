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

package org.apache.ignite.internal.processors.cache.persistence.baseline;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests cluster activation events.
 */
public class ClusterActivationEventTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setIncludeEventTypes(EventType.EVTS_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterActivation() throws Exception {
        ClusterActivationTestTask task = new ClusterActivationTestTask() {
            @Override public void execute(IgniteCluster cluster) throws Exception {
                deactivateCluster(cluster);

                activateCluster(cluster);
            }
        };

        IgnitePredicate<? extends Event> lsnr = (evt) -> {
            System.out.println("Received event [id=" + evt.id().toString() + ", type=" + evt.type() + ']' + ", msg=" + evt.message() + ']');
            return true;
        };

        checkClusterActivation(task, lsnr, EventType.EVT_CLUSTER_ACTIVATED, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterDeactivation() throws Exception {
        ClusterActivationTestTask task = new ClusterActivationTestTask() {
            @Override public void execute(IgniteCluster cluster) throws Exception {
                deactivateCluster(cluster);
            }
        };

        IgnitePredicate<? extends Event> lsnr = (evt) -> {
            System.out.println("Received event [id=" + evt.id().toString() + ", type=" + evt.type() + ']' + ", msg=" + evt.message() + ']');
            return true;
        };

        checkClusterActivation(task, lsnr, EventType.EVT_CLUSTER_DEACTIVATED, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterDoubleActivation() throws Exception {
        ClusterActivationTestTask task = new ClusterActivationTestTask() {
            @Override public void execute(IgniteCluster cluster) throws Exception {
                deactivateCluster(cluster);

                activateCluster(cluster);

                activateCluster(cluster);
            }
        };

        IgnitePredicate<? extends Event> lsnr = (evt) -> {
            System.out.println("Received event [id=" + evt.id().toString() + ", type=" + evt.type() + ']' + ", msg=" + evt.message() + ']');
            return true;
        };

        checkClusterActivation(task, lsnr, EventType.EVT_CLUSTER_ACTIVATED, 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterActivationListenerSleep() throws Exception {
        ClusterActivationTestTask task = new ClusterActivationTestTask() {
            @Override public void execute(IgniteCluster cluster) throws Exception {
                deactivateCluster(cluster);

                activateCluster(cluster);
            }
        };

        IgnitePredicate<? extends Event> lsnr = (evt) -> {
            try {
                Thread.sleep(10000);
            }
            catch (InterruptedException e) {
                throw new IgniteInterruptedException(e);
            }

            System.out.println("Received event [id=" + evt.id().toString() + ", type=" + evt.type() + ']' + ", msg=" + evt.message() + ']');

            return true;
        };

        checkClusterActivation(task, lsnr, EventType.EVT_CLUSTER_ACTIVATED, 1);
    }

    /**
     * @param task Test.
     * @param lsnr Listener.
     * @param evt Event type.
     * @param evtCnt Events count.
     * @throws Exception If failed.
     */
    private void checkClusterActivation(
        ClusterActivationTestTask task,
        IgnitePredicate<? extends Event> lsnr,
        int evt,
        int evtCnt
    ) throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            ignite1.events().localListen(lsnr, evt);
            ignite2.events().localListen(lsnr, evt);

            IgniteCluster cluster = ignite1.cluster();

            assert cluster.active();

            task.execute(cluster);

            assertEventsCount(ignite1, evt, evtCnt);

            assertEventsCount(ignite2, evt, evtCnt);
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * @param ignite Ignite instance
     * @param evt Event
     * @param cnt Count
     */
    private void assertEventsCount(Ignite ignite, int evt, int cnt) {
        Collection<Event> evts = ignite.events().localQuery(F.alwaysTrue(), evt);

        assert evts != null;
        assert evts.size() == cnt;
    }

    /**
     * @param cluster Cluster
     */
    private void activateCluster(IgniteCluster cluster) throws InterruptedException {
        cluster.active(true);

        Thread.sleep(200);

        assert cluster.active();
    }

    /**
     * @param cluster Cluster
     */
    private void deactivateCluster(IgniteCluster cluster) throws InterruptedException {
        cluster.active(false);

        Thread.sleep(200);

        assert !cluster.active();
    }

    /**
     * Cluster activation test task interface
     */
    private interface ClusterActivationTestTask {
        /**
         * @param cluster Cluster
         * @throws Exception If failed.
         */
        void execute(IgniteCluster cluster) throws Exception;
    }
}
