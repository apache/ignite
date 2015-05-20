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

    /** Startup sctipt path. */
    public static final String STARTUP_SCRIPT = "/home/ignite/startup.sh";

    /** Cpus. */
    public static final String CPUS = "cpus";

    /** Mem. */
    public static final String MEM = "mem";

    /** Disk. */
    public static final String DISK = "disk";

    /** Default port range. */
    public static final String DEFAULT_PORT = ":47500..47510";

    /** Delimiter to use in IP names. */
    public static final String DELIM = ",";

    /** Logger. */
    private static final Logger log = LoggerFactory.getLogger(IgniteScheduler.class);

    /** Mutex. */
    private static final Object mux = new Object();

    /** ID generator. */
    private AtomicInteger taskIdGenerator = new AtomicInteger();

    /** Task on host. */
    private Map<String, IgniteTask> tasks = new HashMap<>();

    /** Cluster resources. */
    private ClusterResources clusterLimit;

    /**
     * @param clusterLimit Resources limit.
     */
    public IgniteScheduler(ClusterResources clusterLimit) {
        this.clusterLimit = clusterLimit;
    }

    /** {@inheritDoc} */
    @Override public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID,
        Protos.MasterInfo masterInfo) {
        log.info("registered() master={}:{}, framework={}", masterInfo.getIp(), masterInfo.getPort(), frameworkID);
    }

    /** {@inheritDoc} */
    @Override public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
        log.info("reregistered()");
    }

    /** {@inheritDoc} */
    @Override public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> offers) {
        synchronized (mux) {
            log.info("resourceOffers() with {} offers", offers.size());

            for (Protos.Offer offer : offers) {
                IgniteTask igniteTask = checkOffer(offer);

                // Decline offer which doesn't match by mem or cpu.
                if (igniteTask == null) {
                    schedulerDriver.declineOffer(offer.getId());

                    continue;
                }

                // Generate a unique task ID.
                Protos.TaskID taskId = Protos.TaskID.newBuilder()
                    .setValue(Integer.toString(taskIdGenerator.incrementAndGet())).build();

                log.info("Launching task {}", taskId.getValue());

                // Create task to run.
                Protos.TaskInfo task = createTask(offer, igniteTask, taskId);

                schedulerDriver.launchTasks(Collections.singletonList(offer.getId()),
                    Collections.singletonList(task),
                    Protos.Filters.newBuilder().setRefuseSeconds(1).build());

                tasks.put(taskId.getValue(), igniteTask);
            }
        }
    }

    /**
     * Create Task.
     *
     * @param offer Offer.
     * @param igniteTask Task description.
     * @param taskId Task id.
     * @return Task.
     */
    protected Protos.TaskInfo createTask(Protos.Offer offer, IgniteTask igniteTask, Protos.TaskID taskId) {
        // Docker image info.
        Protos.ContainerInfo.DockerInfo.Builder docker = Protos.ContainerInfo.DockerInfo.newBuilder()
            .setImage(IMAGE)
            .setNetwork(Protos.ContainerInfo.DockerInfo.Network.HOST);

        // Container info.
        Protos.ContainerInfo.Builder cont = Protos.ContainerInfo.newBuilder();
        cont.setType(Protos.ContainerInfo.Type.DOCKER);
        cont.setDocker(docker.build());

        return Protos.TaskInfo.newBuilder()
            .setName("Ignite node " + taskId.getValue())
            .setTaskId(taskId)
            .setSlaveId(offer.getSlaveId())
            .addResources(Protos.Resource.newBuilder()
                .setName(CPUS)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(igniteTask.cpuCores())))
            .addResources(Protos.Resource.newBuilder()
                .setName(MEM)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(igniteTask.mem())))
            .setContainer(cont)
            .setCommand(Protos.CommandInfo.newBuilder()
                .setShell(false)
                .addArguments(STARTUP_SCRIPT)
                .addArguments(String.valueOf((int) igniteTask.mem()))
                .addArguments(getAddress()))
            .build();
    }

    /**
     * @return Address running nodes.
     */
    protected String getAddress() {
        if (tasks.isEmpty())
            return "";

        StringBuilder sb = new StringBuilder();

        for (IgniteTask task : tasks.values())
            sb.append(task.host()).append(DEFAULT_PORT).append(DELIM);

        return sb.substring(0, sb.length() - 1);
    }

    /**
     * Check slave resources and return resources infos.
     *
     * @param offer Offer request.
     * @return Ignite task description.
     */
    private IgniteTask checkOffer(Protos.Offer offer) {
        // Check that limit on running nodes.
        if (!checkLimit(clusterLimit.instances(), tasks.size()))
            return null;

        double cpus = -1;
        double mem = -1;
        double disk = -1;

        // Collect resource on slave.
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
            else if (resource.getName().equals(DISK))
                if (resource.getType().equals(Protos.Value.Type.SCALAR))
                    disk = resource.getScalar().getValue();
                else
                    log.debug("Disk resource was not a scalar: " + resource.getType().toString());
        }

        // Check that slave satisfies min requirements.
        if (cpus < clusterLimit.minCpuPerNode()  && mem < clusterLimit.minMemoryPerNode() ) {
            log.info("Offer not sufficient for slave request:\n" + offer.getResourcesList().toString() +
                "\n" + offer.getAttributesList().toString() +
                "\nRequested for slave:\n" +
                "  cpus:  " + cpus + "\n" +
                "  mem:   " + mem);

            return null;
        }

        double totalCpus = 0;
        double totalMem = 0;
        double totalDisk = 0;

        // Collect occupied resources.
        for (IgniteTask task : tasks.values()) {
            totalCpus += task.cpuCores();
            totalMem += task.mem();
            totalDisk += task.disk();
        }

        cpus = clusterLimit.cpus() == ClusterResources.DEFAULT_VALUE ? cpus :
            Math.min(clusterLimit.cpus() - totalCpus, cpus);
        mem = clusterLimit.memory() == ClusterResources.DEFAULT_VALUE ? mem :
            Math.min(clusterLimit.memory() - totalMem, mem);
        disk = clusterLimit.disk() == ClusterResources.DEFAULT_VALUE ? disk :
            Math.min(clusterLimit.disk() - totalDisk, disk);

        if (cpus > 0 && mem > 0)
            return new IgniteTask(offer.getHostname(), cpus, mem, disk);
        else {
            log.info("Offer not sufficient for slave request:\n" + offer.getResourcesList().toString() +
                "\n" + offer.getAttributesList().toString() +
                "\nRequested for slave:\n" +
                "  cpus:  " + cpus + "\n" +
                "  mem:   " + mem);

            return null;
        }
    }

    /**
     * @param limit Limit.
     * @param value Value.
     * @return {@code True} if limit isn't violated else {@code false}.
     */
    private boolean checkLimit(double limit, double value) {
        return limit == ClusterResources.DEFAULT_VALUE || limit <= value;
    }

    /** {@inheritDoc} */
    @Override public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {
        log.info("offerRescinded()");
    }

    /** {@inheritDoc} */
    @Override public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
        final String taskId = taskStatus.getTaskId().getValue();

        log.info("statusUpdate() task {} is in state {}", taskId, taskStatus.getState());

        switch (taskStatus.getState()) {
            case TASK_FAILED:
            case TASK_FINISHED:
                synchronized (mux) {
                    IgniteTask failedTask = tasks.remove(taskId);

                    if (failedTask != null) {
                        List<Protos.Request> requests = new ArrayList<>();

                        Protos.Request request = Protos.Request.newBuilder()
                            .addResources(Protos.Resource.newBuilder()
                                .setType(Protos.Value.Type.SCALAR)
                                .setName(MEM)
                                .setScalar(Protos.Value.Scalar.newBuilder().setValue(failedTask.mem())))
                            .addResources(Protos.Resource.newBuilder()
                                .setType(Protos.Value.Type.SCALAR)
                                .setName(CPUS)
                                .setScalar(Protos.Value.Scalar.newBuilder().setValue(failedTask.cpuCores())))
                            .build();

                        requests.add(request);

                        schedulerDriver.requestResources(requests);
                    }
                }
                break;
        }
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
}
