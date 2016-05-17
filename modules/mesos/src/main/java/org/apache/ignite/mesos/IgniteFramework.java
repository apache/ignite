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

import com.google.protobuf.ByteString;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.mesos.resource.IgniteProvider;
import org.apache.ignite.mesos.resource.JettyServer;
import org.apache.ignite.mesos.resource.ResourceHandler;
import org.apache.ignite.mesos.resource.ResourceProvider;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;

/**
 * Ignite mesos framework.
 */
public class IgniteFramework {
    /** */
    public static final Logger log = Logger.getLogger(IgniteFramework.class.getSimpleName());

    /** Framework name. */
    public static final String IGNITE_FRAMEWORK_NAME = "Ignite";

    /**
     * Main methods has only one optional parameter - path to properties files.
     *
     * @param args Args.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        final int frameworkFailoverTimeout = 0;

        // Have Mesos fill in the current user.
        Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder()
            .setName(IGNITE_FRAMEWORK_NAME)
            .setUser("")
            .setFailoverTimeout(frameworkFailoverTimeout);

        if (System.getenv("MESOS_CHECKPOINT") != null) {
            log.info("Enabling checkpoint for the framework");

            frameworkBuilder.setCheckpoint(true);
        }

        ClusterProperties clusterProps = ClusterProperties.from(args.length >= 1 ? args[0] : null);

        String baseUrl = String.format("http://%s:%d", clusterProps.httpServerHost(), clusterProps.httpServerPort());

        JettyServer httpSrv = new JettyServer();

        httpSrv.start(
            new InetSocketAddress(clusterProps.httpServerHost(), clusterProps.httpServerPort()),
            new ResourceHandler(clusterProps.userLibs(), clusterProps.igniteCfg(), clusterProps.igniteWorkDir())
        );

        ResourceProvider provider = new ResourceProvider();

        IgniteProvider igniteProvider = new IgniteProvider(clusterProps.igniteWorkDir());

        provider.init(clusterProps, igniteProvider, baseUrl);

        // Create the scheduler.
        Scheduler scheduler = new IgniteScheduler(clusterProps, provider);

        // Create the driver.
        MesosSchedulerDriver driver;

        if (System.getenv("MESOS_AUTHENTICATE") != null) {
            log.info("Enabling authentication for the framework");

            if (System.getenv("DEFAULT_PRINCIPAL") == null) {
                log.log(Level.SEVERE, "Expecting authentication principal in the environment");

                System.exit(1);
            }

            if (System.getenv("DEFAULT_SECRET") == null) {
                log.log(Level.SEVERE, "Expecting authentication secret in the environment");

                System.exit(1);
            }

            Protos.Credential cred = Protos.Credential.newBuilder()
                .setPrincipal(System.getenv("DEFAULT_PRINCIPAL"))
                .setSecret(ByteString.copyFrom(System.getenv("DEFAULT_SECRET").getBytes()))
                .build();

            frameworkBuilder.setPrincipal(System.getenv("DEFAULT_PRINCIPAL"));

            driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), clusterProps.masterUrl(),
                cred);
        }
        else {
            frameworkBuilder.setPrincipal("ignite-framework-java");

            driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), clusterProps.masterUrl());
        }

        int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;

        httpSrv.stop();

        // Ensure that the driver process terminates.
        driver.stop();

        System.exit(status);
    }
}