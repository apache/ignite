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

package org.apache.ignite.mesos;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import junit.framework.TestCase;
import org.apache.ignite.mesos.resource.ResourceProvider;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

/**
 * Scheduler tests.
 */
public class IgniteSchedulerSelfTest extends TestCase {
    /** */
    private IgniteScheduler scheduler;

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();

        ClusterProperties clustProp = new ClusterProperties();

        scheduler = new IgniteScheduler(clustProp, new ResourceProvider() {
            @Override public String configName() {
                return "config.xml";
            }

            @Override public String igniteUrl() {
                return "ignite.jar";
            }

            @Override public String igniteConfigUrl() {
                return "config.xml";
            }

            @Override public Collection<String> resourceUrl() {
                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testHostRegister() throws Exception {
        Protos.Offer offer = createOffer("hostname", 4, 1024);

        DriverMock mock = new DriverMock();

        scheduler.resourceOffers(mock, Collections.singletonList(offer));

        assertNotNull(mock.launchedTask);
        assertEquals(1, mock.launchedTask.size());

        Protos.TaskInfo taskInfo = mock.launchedTask.iterator().next();

        assertEquals(4.0, resources(taskInfo.getResourcesList(), IgniteScheduler.CPU));
        assertEquals(1024.0, resources(taskInfo.getResourcesList(), IgniteScheduler.MEM));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeclineByCpu() throws Exception {
        Protos.Offer offer = createOffer("hostname", 4, 1024);

        DriverMock mock = new DriverMock();

        ClusterProperties clustProp = new ClusterProperties();
        clustProp.cpus(2);

        scheduler.setClusterProps(clustProp);

        scheduler.resourceOffers(mock, Collections.singletonList(offer));

        assertNotNull(mock.launchedTask);
        assertEquals(1, mock.launchedTask.size());

        Protos.TaskInfo taskInfo = mock.launchedTask.iterator().next();

        assertEquals(2.0, resources(taskInfo.getResourcesList(), IgniteScheduler.CPU));
        assertEquals(1024.0, resources(taskInfo.getResourcesList(), IgniteScheduler.MEM));

        mock.clear();

        scheduler.resourceOffers(mock, Collections.singletonList(offer));

        assertNull(mock.launchedTask);

        Protos.OfferID declinedOffer = mock.declinedOffer;

        assertEquals(offer.getId(), declinedOffer);
    }


    /**
     * @throws Exception If failed.
     */
    public void testDeclineByMem() throws Exception {
        Protos.Offer offer = createOffer("hostname", 4, 1024);

        DriverMock mock = new DriverMock();

        ClusterProperties clustProp = new ClusterProperties();
        clustProp.memory(512);

        scheduler.setClusterProps(clustProp);

        scheduler.resourceOffers(mock, Collections.singletonList(offer));

        assertNotNull(mock.launchedTask);
        assertEquals(1, mock.launchedTask.size());

        Protos.TaskInfo taskInfo = mock.launchedTask.iterator().next();

        assertEquals(4.0, resources(taskInfo.getResourcesList(), IgniteScheduler.CPU));
        assertEquals(512.0, resources(taskInfo.getResourcesList(), IgniteScheduler.MEM));

        mock.clear();

        scheduler.resourceOffers(mock, Collections.singletonList(offer));

        assertNull(mock.launchedTask);

        Protos.OfferID declinedOffer = mock.declinedOffer;

        assertEquals(offer.getId(), declinedOffer);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeclineByMemCpu() throws Exception {
        Protos.Offer offer = createOffer("hostname", 1, 1024);

        DriverMock mock = new DriverMock();

        ClusterProperties clustProp = new ClusterProperties();
        clustProp.cpus(4);
        clustProp.memory(2000);

        scheduler.setClusterProps(clustProp);

        double totalMem = 0, totalCpu = 0;

        for (int i = 0; i < 2; i++) {
            scheduler.resourceOffers(mock, Collections.singletonList(offer));

            assertNotNull(mock.launchedTask);
            assertEquals(1, mock.launchedTask.size());

            Protos.TaskInfo taskInfo = mock.launchedTask.iterator().next();

            totalCpu += resources(taskInfo.getResourcesList(), IgniteScheduler.CPU);
            totalMem += resources(taskInfo.getResourcesList(), IgniteScheduler.MEM);

            mock.clear();
        }

        assertEquals(2.0, totalCpu);
        assertEquals(2000.0, totalMem);

        scheduler.resourceOffers(mock, Collections.singletonList(offer));

        assertNull(mock.launchedTask);

        Protos.OfferID declinedOffer = mock.declinedOffer;

        assertEquals(offer.getId(), declinedOffer);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeclineByCpuMinRequirements() throws Exception {
        Protos.Offer offer = createOffer("hostname", 8, 10240);

        DriverMock mock = new DriverMock();

        ClusterProperties clustProp = new ClusterProperties();
        clustProp.minCpuPerNode(12);

        scheduler.setClusterProps(clustProp);

        scheduler.resourceOffers(mock, Collections.singletonList(offer));

        assertNotNull(mock.declinedOffer);

        assertEquals(offer.getId(), mock.declinedOffer);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeclineByMemMinRequirements() throws Exception {
        Protos.Offer offer = createOffer("hostname", 8, 10240);

        DriverMock mock = new DriverMock();

        ClusterProperties clustProp = new ClusterProperties();
        clustProp.minMemoryPerNode(15000);

        scheduler.setClusterProps(clustProp);

        scheduler.resourceOffers(mock, Collections.singletonList(offer));

        assertNotNull(mock.declinedOffer);

        assertEquals(offer.getId(), mock.declinedOffer);
    }

    /**
     * @throws Exception If failed.
     */
    public void testHosthameConstraint() throws Exception {
        Protos.Offer offer = createOffer("hostname", 8, 10240);

        DriverMock mock = new DriverMock();

        ClusterProperties clustProp = new ClusterProperties();
        clustProp.hostnameConstraint(Pattern.compile("hostname"));

        scheduler.setClusterProps(clustProp);

        scheduler.resourceOffers(mock, Collections.singletonList(offer));

        assertNotNull(mock.declinedOffer);

        assertEquals(offer.getId(), mock.declinedOffer);

        offer = createOffer("hostnameAccept", 8, 10240);

        scheduler.resourceOffers(mock, Collections.singletonList(offer));

        assertNotNull(mock.launchedTask);
        assertEquals(1, mock.launchedTask.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPerNode() throws Exception {
        Protos.Offer offer = createOffer("hostname", 8, 1024);

        DriverMock mock = new DriverMock();

        ClusterProperties clustProp = new ClusterProperties();
        clustProp.memoryPerNode(1024);
        clustProp.cpusPerNode(2);

        scheduler.setClusterProps(clustProp);

        scheduler.resourceOffers(mock, Collections.singletonList(offer));

        assertNotNull(mock.launchedTask);

        Protos.TaskInfo taskInfo = mock.launchedTask.iterator().next();

        assertEquals(2.0, resources(taskInfo.getResourcesList(), IgniteScheduler.CPU));
        assertEquals(1024.0, resources(taskInfo.getResourcesList(), IgniteScheduler.MEM));

        mock.clear();

        offer = createOffer("hostname", 1, 2048);

        scheduler.resourceOffers(mock, Collections.singletonList(offer));

        assertNull(mock.launchedTask);

        assertNotNull(mock.declinedOffer);
        assertEquals(offer.getId(), mock.declinedOffer);

        mock.clear();

        offer = createOffer("hostname", 4, 512);

        scheduler.resourceOffers(mock, Collections.singletonList(offer));

        assertNull(mock.launchedTask);

        assertNotNull(mock.declinedOffer);
        assertEquals(offer.getId(), mock.declinedOffer);
    }

    /**
     * @param resourceType Resource type.
     * @return Value.
     */
    private Double resources(List<Protos.Resource> resources, String resourceType) {
        for (Protos.Resource resource : resources) {
            if (resource.getName().equals(resourceType))
                return resource.getScalar().getValue();
        }

        return null;
    }

    /**
     * @param hostname Hostname
     * @param cpu Cpu count.
     * @param mem Mem size.
     * @return Offer.
     */
    private Protos.Offer createOffer(String hostname, double cpu, double mem) {
        return Protos.Offer.newBuilder()
            .setId(Protos.OfferID.newBuilder().setValue("1"))
            .setSlaveId(Protos.SlaveID.newBuilder().setValue("1"))
            .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("1"))
            .setHostname(hostname)
            .addResources(Protos.Resource.newBuilder()
                .setType(Protos.Value.Type.SCALAR)
                .setName(IgniteScheduler.CPU)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpu).build())
                .build())
            .addResources(Protos.Resource.newBuilder()
                .setType(Protos.Value.Type.SCALAR)
                .setName(IgniteScheduler.MEM)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(mem).build())
                .build())
            .build();
    }

    /**
     * No-op implementation.
     */
    public static class DriverMock implements SchedulerDriver {
        /** */
        Collection<Protos.TaskInfo> launchedTask;

        /** */
        Protos.OfferID declinedOffer;

        /**
         * Clears launched task.
         */
        public void clear() {
            launchedTask = null;
            declinedOffer = null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status start() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status stop(boolean failover) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status stop() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status abort() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status join() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status run() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status requestResources(Collection<Protos.Request> requests) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status launchTasks(Collection<Protos.OfferID> offerIds,
            Collection<Protos.TaskInfo> tasks, Protos.Filters filters) {
            launchedTask = tasks;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status launchTasks(Collection<Protos.OfferID> offerIds,
            Collection<Protos.TaskInfo> tasks) {
            launchedTask = tasks;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status launchTasks(Protos.OfferID offerId, Collection<Protos.TaskInfo> tasks,
            Protos.Filters filters) {
            launchedTask = tasks;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status launchTasks(Protos.OfferID offerId, Collection<Protos.TaskInfo> tasks) {
            launchedTask = tasks;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status killTask(Protos.TaskID taskId) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status declineOffer(Protos.OfferID offerId, Protos.Filters filters) {
            declinedOffer = offerId;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status declineOffer(Protos.OfferID offerId) {
            declinedOffer = offerId;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status reviveOffers() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status acknowledgeStatusUpdate(Protos.TaskStatus status) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status sendFrameworkMessage(Protos.ExecutorID executorId, Protos.SlaveID slaveId,
            byte[] data) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status reconcileTasks(Collection<Protos.TaskStatus> statuses) {
            return null;
        }
    }
}