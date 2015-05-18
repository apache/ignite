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

import junit.framework.*;
import org.apache.mesos.*;

import java.util.*;

/**
 * Scheduler tests.
 */
public class IgniteSchedulerSelfTest extends TestCase {
    /** */
    private IgniteScheduler scheduler;

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        super.setUp();

        scheduler = new IgniteScheduler();
    }

    /**
     * @throws Exception If failed.
     */
    public void testHostRegister() throws Exception {
        //Protos.Offer offer = createOffer("hostname", 4, 1024);

        //scheduler.resourceOffers(DriverStub.INSTANCE, Lists.);
    }

    private Protos.Offer createOffer(String hostname, double cpu, double mem) {
        return Protos.Offer.newBuilder()
            .setSlaveId(Protos.SlaveID.newBuilder().setValue("1").build())
            .setHostname(hostname)
            .addResources(Protos.Resource.newBuilder()
                .setName(IgniteScheduler.CPUS)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpu).build())
                .build())
            .addResources(Protos.Resource.newBuilder()
                .setName(IgniteScheduler.MEM)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(mem).build())
                .build())
            .build();
    }

    /**
     * No-op implementation.
     */
    public static class DriverStub implements SchedulerDriver {
        private static final DriverStub INSTANCE = new DriverStub();

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
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status launchTasks(Collection<Protos.OfferID> offerIds,
            Collection<Protos.TaskInfo> tasks) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status launchTasks(Protos.OfferID offerId, Collection<Protos.TaskInfo> tasks,
            Protos.Filters filters) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status launchTasks(Protos.OfferID offerId, Collection<Protos.TaskInfo> tasks) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status killTask(Protos.TaskID taskId) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status declineOffer(Protos.OfferID offerId, Protos.Filters filters) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Protos.Status declineOffer(Protos.OfferID offerId) {
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