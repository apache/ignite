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

package org.apache.ignite.internal;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;

/**
 * This class run some basic network diagnostic tests in the background and
 * reports errors or suspicious results.
 */
final class GridDiagnostic {
    /** */
    private static final int REACH_TIMEOUT = 2000;

    /**
     * Ensure singleton.
     */
    private GridDiagnostic() {
        // No-op.
    }

    /**
     * @param gridName Grid instance name. Can be {@code null}.
     * @param exec Executor service.
     * @param parentLog Parent logger.
     */
    static void runBackgroundCheck(String gridName, Executor exec, IgniteLogger parentLog) {
        assert exec != null;
        assert parentLog != null;

        final IgniteLogger log = parentLog.getLogger(GridDiagnostic.class);

        try {
            exec.execute(new GridWorker(gridName, "grid-diagnostic-1", log) {
                @Override public void body() {
                    try {
                        InetAddress locHost = U.getLocalHost();

                        if (!locHost.isReachable(REACH_TIMEOUT)) {
                            U.warn(log, "Default local host is unreachable. This may lead to delays on " +
                                "grid network operations. Check your OS network setting to correct it.",
                                "Default local host is unreachable.");
                        }
                    }
                    catch (IOException ignore) {
                        U.warn(log, "Failed to perform network diagnostics. It is usually caused by serious " +
                            "network configuration problem. Check your OS network setting to correct it.",
                            "Failed to perform network diagnostics.");
                    }
                }
            });

            exec.execute(new GridWorker(gridName, "grid-diagnostic-2", log) {
                @Override public void body() {
                    try {
                        InetAddress locHost = U.getLocalHost();

                        if (locHost.isLoopbackAddress()) {
                            U.warn(log, "Default local host is a loopback address. This can be a sign of " +
                                "potential network configuration problem.",
                                "Default local host is a loopback address.");
                        }
                    }
                    catch (IOException ignore) {
                        U.warn(log, "Failed to perform network diagnostics. It is usually caused by serious " +
                            "network configuration problem. Check your OS network setting to correct it.",
                            "Failed to perform network diagnostics.");
                    }
                }
            });

            exec.execute(new GridWorker(gridName, "grid-diagnostic-4", log) {
                @Override public void body() {
                    // Sufficiently tested OS.
                    if (!U.isSufficientlyTestedOs()) {
                        U.warn(log, "This operating system has been tested less rigorously: " + U.osString() +
                            ". Our team will appreciate the feedback if you experience any problems running " +
                            "ignite in this environment.",
                            "This OS is tested less rigorously: " + U.osString());
                    }
                }
            });

            exec.execute(new GridWorker(gridName, "grid-diagnostic-5", log) {
                @Override public void body() {
                    // Fix for GG-1075.
                    if (F.isEmpty(U.allLocalMACs()))
                        U.warn(log, "No live network interfaces detected. If IP-multicast discovery is used - " +
                            "make sure to add 127.0.0.1 as a local address.",
                            "No live network interfaces. Add 127.0.0.1 as a local address.");
                }
            });

            exec.execute(new GridWorker(gridName, "grid-diagnostic-6", log) {
                @Override public void body() {
                    if (System.getProperty("com.sun.management.jmxremote") != null) {
                        String portStr = System.getProperty("com.sun.management.jmxremote.port");

                        if (portStr != null)
                            try {
                                Integer.parseInt(portStr);

                                return;
                            }
                            catch (NumberFormatException ignore) {
                                // No-op.
                            }

                        U.warn(log, "JMX remote management is enabled but JMX port is either not set or invalid. " +
                            "Check system property 'com.sun.management.jmxremote.port' to make sure it specifies " +
                            "valid TCP/IP port.", "JMX remote port is invalid - JMX management is off.");
                    }
                }
            });

            final long HALF_GB = 512/*MB*/ * 1024 * 1024;

            exec.execute(new GridWorker(gridName, "grid-diagnostic-7", log) {
                @Override public void body() {
                    long initBytes = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getInit();
                    long initMb = initBytes / 1024 / 1024;

                    if (initBytes < HALF_GB)
                        U.quietAndWarn(log,
                            String.format("Initial heap size is %dMB (should be no less than 512MB, " +
                                "use -Xms512m -Xmx512m).", initMb));
                }
            });
        }
        catch (RejectedExecutionException e) {
            U.error(log, "Failed to start background network diagnostics check due to thread pool execution " +
                "rejection. In most cases it indicates a severe configuration problem with Ignite.",
                "Failed to start background network diagnostics.", e);
        }
    }
}