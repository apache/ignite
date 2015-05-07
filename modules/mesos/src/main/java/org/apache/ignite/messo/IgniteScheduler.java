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

package org.apache.ignite.messo;

import org.apache.mesos.*;
import org.slf4j.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * TODO
 */
public class IgniteScheduler implements Scheduler {
    /** Docker image name. */
    public static final String IMAGE = "apacheignite/ignite-docker";

    /** Cpus. */
    public static final String CPUS = "cpus";

    /** Mem. */
    public static final String MEM = "mem";

    /** ID generator. */
    private AtomicInteger taskIdGenerator = new AtomicInteger();

    /** Logger. */
    private static final Logger log = LoggerFactory.getLogger(IgniteScheduler.class);

    /** Min of memory required. */
    public static final int MIN_MEMORY = 256;

    /** {@inheritDoc} */
    @Override public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID,
        Protos.MasterInfo masterInfo) {
        log.info("registered() master={}:{}, framework={}", masterInfo.getIp(), masterInfo.getPort(), frameworkID);
    }

    /** {@inheritDoc} */
    @Override public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
        log.info("reregistered");
    }

    /** {@inheritDoc} */
    @Override public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> offers) {
        log.info("resourceOffers() with {} offers", offers.size());

        List<Protos.OfferID> offerIDs = new ArrayList<>(offers.size());
        List<Protos.TaskInfo> tasks = new ArrayList<>(offers.size());

        for (Protos.Offer offer : offers) {
            Pair<Double, Double> cpuMem = checkOffer(offer);

            //
            if (cpuMem == null)
                continue;

            // Generate a unique task ID.
            Protos.TaskID taskId = Protos.TaskID.newBuilder()
                .setValue(Integer.toString(taskIdGenerator.incrementAndGet())).build();

            log.info("Launching task {}", taskId.getValue());

            // Docker image info.
            Protos.ContainerInfo.DockerInfo.Builder docker = Protos.ContainerInfo.DockerInfo.newBuilder()
                .setImage(IMAGE)
                .setNetwork(Protos.ContainerInfo.DockerInfo.Network.HOST);

            // Container info.
            Protos.ContainerInfo.Builder cont = Protos.ContainerInfo.newBuilder();
            cont.setType(Protos.ContainerInfo.Type.DOCKER);
            cont.setDocker(docker.build());

            // Create task to run.
            Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
                .setName("task " + taskId.getValue())
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .addResources(Protos.Resource.newBuilder()
                    .setName(CPUS)
                    .setType(Protos.Value.Type.SCALAR)
                    .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpuMem._1)))
                .addResources(Protos.Resource.newBuilder()
                    .setName(MEM)
                    .setType(Protos.Value.Type.SCALAR)
                    .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpuMem._2)))
                .setContainer(cont)
                .setCommand(Protos.CommandInfo.newBuilder().setShell(false))
                .build();

            offerIDs.add(offer.getId());
            tasks.add(task);
        }

        schedulerDriver.launchTasks(offerIDs, tasks, Protos.Filters.newBuilder().setRefuseSeconds(1).build());
    }

    /**
     * Check slave resources and return resources infos.
     *
     * @param offer Offer request.
     * @return Pair where first is cpus, second is memory.
     */
    private Pair<Double, Double> checkOffer(Protos.Offer offer) {
        double cpus = -1;
        double mem = -1;

        for (Protos.Resource resource : offer.getResourcesList()) {
            if (resource.getName().equals(CPUS)) {
                if (resource.getType().equals(Protos.Value.Type.SCALAR))
                    cpus = resource.getScalar().getValue();
                else
                    log.debug("Cpus resource was not a scalar: " + resource.getType().toString());
            }
            else if (resource.getName().equals(MEM)) {
                if (resource.getType().equals(Protos.Value.Type.SCALAR))
                    mem = resource.getScalar().getValue();
                else
                    log.debug("Mem resource was not a scalar: " + resource.getType().toString());
            }
            else if (resource.getName().equals("disk"))
                log.debug("Ignoring disk resources from offer");
        }

        if (cpus < 0)
            log.debug("No cpus resource present");
        if (mem < 0)
            log.debug("No mem resource present");

        if (cpus >= 1 && MIN_MEMORY <= mem)
            return new Pair<>(cpus, mem);
        else {
            log.info("Offer not sufficient for slave request:\n" + offer.getResourcesList().toString() +
                "\n" + offer.getAttributesList().toString() +
                "\nRequested for slave:\n" +
                "  cpus:  " + cpus + "\n" +
                "  mem:   " + mem);

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {
        log.info("offerRescinded()");
    }

    /** {@inheritDoc} */
    @Override public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
        log.info("statusUpdate() task {} ", taskStatus);
    }

    /** {@inheritDoc} */
    @Override public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID,
        Protos.SlaveID slaveID, byte[] bytes) {
        log.info("frameworkMessage()");
    }

    /** {@inheritDoc} */
    @Override public void disconnected(SchedulerDriver schedulerDriver) {
        log.info("disconnected()");
    }

    /** {@inheritDoc} */
    @Override public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {
        log.info("slaveLost()");
    }

    /** {@inheritDoc} */
    @Override public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID,
        Protos.SlaveID slaveID, int i) {
        log.info("executorLost()");
    }

    /** {@inheritDoc} */
    @Override public void error(SchedulerDriver schedulerDriver, String s) {
        log.error("error() {}", s);
    }

    /**
     * Tuple.
     */
    public static class Pair<A, B> {
        /** */
        public final A _1;

        /** */
        public final B _2;

        /**
         *
         */
        public Pair(A _1, B _2) {
            this._1 = _1;
            this._2 = _2;
        }
    }
}
