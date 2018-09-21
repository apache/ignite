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
    private static final Logger log = Logger.getLogger(IgniteFramework.class.getSimpleName());

    /** Framework name. */
    private static final String IGNITE_FRAMEWORK_NAME = "Ignite";

    /** MESOS system environment name */
    private static final String MESOS_USER_NAME = "MESOS_USER";

    /** MESOS system environment role */
    private static final String MESOS_ROLE = "MESOS_ROLE";

    /** */
    private static final String MESOS_AUTHENTICATE = "MESOS_AUTHENTICATE";

    /** */
    private static final String DEFAULT_PRINCIPAL = "DEFAULT_PRINCIPAL";

    /** */
    private static final String DEFAULT_SECRET = "DEFAULT_SECRET";

    /** */
    private static final String MESOS_CHECKPOINT = "MESOS_CHECKPOINT";

    /**
     * Main methods has only one optional parameter - path to properties files.
     *
     * @param args Args.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        IgniteFramework igniteFramework = new IgniteFramework();

        ClusterProperties clusterProps = ClusterProperties.from(args.length >= 1 ? args[0] : null);

        String baseUrl = String.format("http://%s:%d", clusterProps.httpServerHost(), clusterProps.httpServerPort());

        JettyServer httpSrv = new JettyServer();

        httpSrv.start(
            new ResourceHandler(clusterProps.userLibs(), clusterProps.igniteCfg(), clusterProps.igniteWorkDir()),
            clusterProps
        );

        ResourceProvider provider = new ResourceProvider();

        IgniteProvider igniteProvider = new IgniteProvider(clusterProps.igniteWorkDir());

        provider.init(clusterProps, igniteProvider, baseUrl);

        // Create the scheduler.
        Scheduler scheduler = new IgniteScheduler(clusterProps, provider);

        // Create the driver.
        MesosSchedulerDriver driver;

        if (System.getenv(MESOS_AUTHENTICATE) != null) {
            log.info("Enabling authentication for the framework");

            if (System.getenv(DEFAULT_PRINCIPAL) == null) {
                log.log(Level.SEVERE, "Expecting authentication principal in the environment");

                System.exit(1);
            }

            if (System.getenv(DEFAULT_SECRET) == null) {
                log.log(Level.SEVERE, "Expecting authentication secret in the environment");

                System.exit(1);
            }

            Protos.Credential cred = Protos.Credential.newBuilder()
                .setPrincipal(System.getenv(DEFAULT_PRINCIPAL))
                .setSecret(System.getenv(DEFAULT_SECRET))
                .build();

            driver = new MesosSchedulerDriver(scheduler, igniteFramework.getFrameworkInfo(), clusterProps.masterUrl(),
                cred);
        }
        else
            driver = new MesosSchedulerDriver(scheduler, igniteFramework.getFrameworkInfo(), clusterProps.masterUrl());

        int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;

        httpSrv.stop();

        // Ensure that the driver process terminates.
        driver.stop();

        System.exit(status);
    }

    /**
     * @return Mesos Protos FrameworkInfo.
     */
    public Protos.FrameworkInfo getFrameworkInfo() throws Exception {
        final int frameworkFailoverTimeout = 0;

        Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder()
            .setName(IGNITE_FRAMEWORK_NAME)
            .setUser(getUser())
            .setRole(getRole())
            .setFailoverTimeout(frameworkFailoverTimeout);

        if (System.getenv(MESOS_CHECKPOINT) != null) {
            log.info("Enabling checkpoint for the framework");

            frameworkBuilder.setCheckpoint(true);
        }

        if (System.getenv(MESOS_AUTHENTICATE) != null)
            frameworkBuilder.setPrincipal(System.getenv(DEFAULT_PRINCIPAL));
        else
            frameworkBuilder.setPrincipal("ignite-framework-java");

        return frameworkBuilder.build();
    }

    /**
     * @return Mesos user name value.
     */
    protected String getUser() {
        String userName = System.getenv(MESOS_USER_NAME);

        return userName != null ? userName : "";
    }

    /**
     * @return Mesos role value.
     */
    protected String getRole() {
        String mesosRole = System.getenv(MESOS_ROLE);

        return isRoleValid(mesosRole) ? mesosRole : "*";
    }

    /**
     * @return Result of Mesos role validation.
     */
    static boolean isRoleValid(String mRole) {
        if (mRole == null || mRole.isEmpty() || mRole.equals(".") || mRole.equals("..") ||
            mRole.startsWith("-") || mRole.contains("/") || mRole.contains("\\") || mRole.contains(" ")) {
            log.severe("Provided mesos role is not valid: [" + mRole +
                "]. Mesos role should be a valid directory name.");

            return false;
        }
        return true;
    }
}