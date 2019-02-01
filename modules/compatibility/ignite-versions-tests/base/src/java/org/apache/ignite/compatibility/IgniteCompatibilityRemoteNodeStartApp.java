/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility;

import com.thoughtworks.xstream.XStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.jetbrains.annotations.NotNull;

/**
 * Application for managing external Ignite node runned by tests.
 */
public class IgniteCompatibilityRemoteNodeStartApp {
    /** */
    public static final String START_NODE_PREFIX = "Started node: ";

    /** */
    public static final String FINISHED_COMMAND_MSG = "Command executed.";

    /** */
    public static final String FAILED_COMMAND_MSG = "Command execution failed.";

    /** */
    public static final String FINISHED_CLOSURE_MSG = "Closure executed.";

    /** */
    public static final String FAILED_CLOSURE_MSG = "Closure execution failed.";

    /** */
    public static final String FAILED_MSG = "GridCompatibilityRemoteNodeStartApp failed";

    /**
     * @param args Arguments.
     */
    public static void main(String[] args) {
        try {
            if (args.length < 1)
                throw new Exception("GridCompatibilityRemoteNodeStartApp requires path to configuration closure");

            String cfgClosurePath = args[0];

            IgniteOutClosure<Ignite> nodeC = readClosureFromFileAndDelete(cfgClosurePath);

            final Ignite node = nodeC.apply();

            System.out.println(START_NODE_PREFIX + node.configuration().getNodeId());
            System.out.flush();

            Scanner scanner = new Scanner(System.in);

            final IgniteLogger log = node.log();

            ScheduledExecutorService cExec = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override public Thread newThread(@NotNull Runnable runnable) {
                    Thread t = new Thread(runnable);

                    t.setName("test-thread");
                    t.setDaemon(true);

                    return  t;
                }
            });

            while (true) {
                String next = scanner.next();

                log.info("Read next command: " + next);

                try {
                    switch (next) {
                        case "stop": {
                            node.close();

                            System.exit(0);
                        }

                        case "dumpDebugInfo": {
                            ((IgniteKernal)node).dumpDebugInfo();

                            System.out.println("Dump threads for " + node.name());

                            U.dumpThreads(null);

                            break;
                        }

                        case "runClosure": {
                            String path = scanner.next();

                            final IgniteInClosure<Ignite> c = readClosureFromFileAndDelete(path);

                            log.info("Execute closure: " + c.getClass().getName());

                            // Run from separate thread to do not block command execution in case of hangs.
                            cExec.submit(new Runnable() {
                                @Override public void run() {
                                    try {
                                        c.apply(node);

                                        log.info("Finished closure: " + c.getClass().getName());

                                        System.out.println(FINISHED_CLOSURE_MSG);
                                        System.out.flush();
                                    }
                                    catch (Throwable e) {
                                        log.error("Closure execution failed: " + e, e);

                                        System.out.println(FAILED_CLOSURE_MSG);
                                        System.out.flush();
                                    }
                                }
                            });

                            continue;
                        }

                        default:
                            throw new Exception("Invalid command: " + next);
                    }

                    log.info("Finished command: " + next);

                    System.out.println(FINISHED_COMMAND_MSG);
                    System.out.flush();
                }
                catch (Throwable e) {
                    System.out.println(FAILED_COMMAND_MSG);
                    System.out.flush();

                    throw e;
                }
            }
        }
        catch (Throwable e) {
            e.printStackTrace(System.out);

            System.out.println(FAILED_MSG);

            System.exit(1);
        }
    }

    /**
     * @param fileName File name.
     * @return Closure.
     * @throws IOException If failed.
     * @param <T> Closure type.
     */
    @SuppressWarnings("unchecked")
    private static <T> T readClosureFromFileAndDelete(String fileName) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(fileName), StandardCharsets.UTF_8)) {
            return (T)new XStream().fromXML(reader);
        }
        finally {
            U.delete(new File(fileName));
        }
    }
}
