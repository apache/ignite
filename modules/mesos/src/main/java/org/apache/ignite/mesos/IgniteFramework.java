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

import com.google.protobuf.*;
import org.apache.mesos.*;

/**
 * TODO
 */
public class IgniteFramework {
    /**
     * @param args Args [host:port] [resource limit]
     */
    public static void main(String[] args) {
        checkArgs(args);

        final int frameworkFailoverTimeout = 0;

        Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder()
            .setName("IgniteFramework")
            .setUser("") // Have Mesos fill in the current user.
            .setFailoverTimeout(frameworkFailoverTimeout); // timeout in seconds

        if (System.getenv("MESOS_CHECKPOINT") != null) {
            System.out.println("Enabling checkpoint for the framework");
            frameworkBuilder.setCheckpoint(true);
        }

        // create the scheduler
        final Scheduler scheduler = new IgniteScheduler(ClusterResources.from(args[1]));

        // create the driver
        MesosSchedulerDriver driver;
        if (System.getenv("MESOS_AUTHENTICATE") != null) {
            System.out.println("Enabling authentication for the framework");

            if (System.getenv("DEFAULT_PRINCIPAL") == null) {
                System.err.println("Expecting authentication principal in the environment");
                System.exit(1);
            }

            if (System.getenv("DEFAULT_SECRET") == null) {
                System.err.println("Expecting authentication secret in the environment");
                System.exit(1);
            }

            Protos.Credential credential = Protos.Credential.newBuilder()
                .setPrincipal(System.getenv("DEFAULT_PRINCIPAL"))
                .setSecret(ByteString.copyFrom(System.getenv("DEFAULT_SECRET").getBytes()))
                .build();

            frameworkBuilder.setPrincipal(System.getenv("DEFAULT_PRINCIPAL"));

            driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0], credential);
        }
        else {
            frameworkBuilder.setPrincipal("ignite-framework-java");

            driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0]);
        }

        int status = driver.run() == Protos.Status.DRIVER_STOPPED ? 0 : 1;

        // Ensure that the driver process terminates.
        driver.stop();

        System.exit(status);
    }

    /**
     * Check input arguments.
     *
     * @param args Arguments.
     */
    private static void checkArgs(String[] args) {
        if (args.length == 0)
            throw new IllegalArgumentException("Illegal arguments.");

        // TODO: add more
    }
}
