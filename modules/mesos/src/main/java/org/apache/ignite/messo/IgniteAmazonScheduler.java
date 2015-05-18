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

/**
 * TODO
 */
public class IgniteAmazonScheduler extends IgniteScheduler {
    /** */
    public static final String AMAZON = "amazon";

    /** Amazon credential. */
    private final String accessKey, secretKey;

    /**
     * Constructor.
     *
     * @param accessKey Access key.
     * @param secretKey Secret key.
     */
    public IgniteAmazonScheduler(String accessKey, String secretKey) {
        assert accessKey != null;
        assert secretKey != null;

        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    /** {@inheritDoc} */
    @Override protected Protos.TaskInfo createTask(Protos.Offer offer, Tuple<Double, Double> cpuMem,
        Protos.TaskID taskId) {
        // Docker image info.
        Protos.ContainerInfo.DockerInfo.Builder docker = Protos.ContainerInfo.DockerInfo.newBuilder()
            .setImage(IMAGE)
            .setNetwork(Protos.ContainerInfo.DockerInfo.Network.HOST);

        // Container info.
        Protos.ContainerInfo.Builder cont = Protos.ContainerInfo.newBuilder();
        cont.setType(Protos.ContainerInfo.Type.DOCKER);
        cont.setDocker(docker.build());

        return Protos.TaskInfo.newBuilder()
            .setName("task " + taskId.getValue())
            .setTaskId(taskId)
            .setSlaveId(offer.getSlaveId())
            .addResources(Protos.Resource.newBuilder()
                .setName(CPUS)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpuMem.get1())))
            .addResources(Protos.Resource.newBuilder()
                .setName(MEM)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpuMem.get2())))
            .setContainer(cont)
            .setCommand(Protos.CommandInfo.newBuilder()
                .setShell(false)
                .addArguments(STARTUP_SCRIPT)
                .addArguments(String.valueOf(cpuMem.get2().intValue()))
                .addArguments(AMAZON)
                .addArguments(accessKey)
                .addArguments(secretKey))
            .build();
    }
}
