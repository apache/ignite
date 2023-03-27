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

package org.apache.ignite.compatibility.testframework.junits;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;
import com.thoughtworks.xstream.XStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.compatibility.testframework.util.CompatibilityTestsUtils;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.multijvm.IgniteNodeRunner;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Runs Ignite node.
 */
public class IgniteCompatibilityNodeRunner extends IgniteNodeRunner {
    /** */
    private static final String IGNITE_COMPATIBILITY_CLOSURE_FILE = System.getProperty("java.io.tmpdir") +
        File.separator + "igniteCompatibilityClosure.tmp_";

    /**
     * Starts {@link Ignite} with test's default configuration.
     *
     * Command-line arguments specification:
     * <pre>
     * args[0] - required - path to closure for tuning IgniteConfiguration before node startup;
     * args[1] - required - name of the starting node;
     * args[2] - required - id of the starting node;
     * args[3] - required - sync-id of a node for synchronization of startup. Must be equals
     * to arg[2] in case of starting the first node in the Ignite cluster;
     * args[4] - required - expected Ignite's version to check at startup;
     * args[5] - optional - path to closure for actions after node startup.
     * </pre>
     *
     * @param args Command-line arguments.
     * @throws Exception In case of an error.
     */
    public static void main(String[] args) throws Exception {
        try {
            X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());

            X.println("Starting Ignite Node... Args=" + Arrays.toString(args));

            if (args.length < 5) {
                throw new IllegalArgumentException("At least five arguments expected:" +
                    " [path/to/closure/file] [ignite-instance-name] [node-id] [sync-node-id] [node-ver]" +
                    " [optional/path/to/closure/file]");
            }

            final Thread watchdog = delayedDumpClasspath();

            IgniteConfiguration cfg = CompatibilityTestsFacade.getConfiguration();

            IgniteInClosure<IgniteConfiguration> cfgClo = readClosureFromFileAndDelete(args[0]);

            cfgClo.apply(cfg);

            final UUID nodeId = UUID.fromString(args[2]);
            final UUID syncNodeId = UUID.fromString(args[3]);
            final IgniteProductVersion expNodeVer = IgniteProductVersion.fromString(args[4]);

            // Ignite instance name and id must be set according to arguments
            // it's used for nodes managing: start, stop etc.
            cfg.setIgniteInstanceName(args[1]);
            cfg.setNodeId(nodeId);

            final Ignite ignite = Ignition.start(cfg);

            assert ignite.cluster().node(syncNodeId) != null : "Node has not joined [id=" + nodeId + "]";

            assert ignite.cluster().localNode().version().compareToIgnoreTimestamp(expNodeVer) == 0 : "Node is of unexpected " +
                "version: [act=" + ignite.cluster().localNode().version() + ", exp=" + expNodeVer + ']';

            // It needs to set private static field 'ignite' of the IgniteNodeRunner class via reflection
            GridTestUtils.setFieldValue(new IgniteNodeRunner(), "ignite", ignite);

            if (args.length == 6) {
                IgniteInClosure<Ignite> clo = readClosureFromFileAndDelete(args[5]);

                clo.apply(ignite);
            }

            X.println(IgniteCompatibilityAbstractTest.SYNCHRONIZATION_LOG_MESSAGE + nodeId);

            watchdog.interrupt();
        }
        catch (Throwable e) {
            e.printStackTrace();

            X.println("Dumping classpath, error occurred: " + e);

            dumpClasspath();

            throw e;
        }
    }

    /**
     * Starts background watchdog thread which will dump main thread stacktrace and classpath dump if main thread
     * will not respond with node startup finished.
     *
     * @return Thread to be interrupted.
     */
    private static Thread delayedDumpClasspath() {
        final Thread mainThread = Thread.currentThread();
        final Runnable target = new Runnable() {
            @Override public void run() {
                try {
                    final int timeout = IgniteCompatibilityAbstractTest.NODE_JOIN_TIMEOUT - 1_000;
                    if (timeout > 0)
                        Thread.sleep(timeout);
                }
                catch (InterruptedException ignored) {
                    //interrupt is correct behaviour
                    return;
                }

                X.println("Ignite startup/Init closure/post configuration closure is probably hanging at");

                for (StackTraceElement ste : mainThread.getStackTrace())
                    X.println("\t" + ste.toString());

                X.println("\nDumping classpath");
                dumpClasspath();
            }
        };

        final Thread thread = new Thread(target);

        thread.setDaemon(true);
        thread.start();

        return thread;
    }

    /**
     * Dumps classpath to output stream.
     */
    private static void dumpClasspath() {
        ClassLoader clsLdr = IgniteCompatibilityNodeRunner.class.getClassLoader();

        for (URL url : CompatibilityTestsUtils.classLoaderUrls(clsLdr))
            X.println("Classpath url: [" + url.getPath() + ']');
    }

    /**
     * Stores {@link IgniteInClosure} to file as xml.
     *
     * @param clo IgniteInClosure.
     * @return A name of file where the closure was stored.
     * @throws IOException In case of an error.
     * @see #readClosureFromFileAndDelete(String)
     */
    @Nullable public static String storeToFile(@Nullable IgniteInClosure clo) throws IOException {
        if (clo == null)
            return null;

        String fileName = IGNITE_COMPATIBILITY_CLOSURE_FILE + clo.hashCode();

        storeToFile(clo, fileName);

        return fileName;
    }

    /**
     * Stores {@link IgniteInClosure} to file as xml.
     *
     * @param clo IgniteInClosure.
     * @param fileName A name of file where the closure was stored.
     * @throws IOException In case of an error.
     * @see #readClosureFromFileAndDelete(String)
     */
    public static void storeToFile(@NotNull IgniteInClosure clo, @NotNull String fileName) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName), StandardCharsets.UTF_8)) {
            new XStream().toXML(clo, writer);
        }
    }

    /**
     * Reads closure from given file name and delete the file after.
     *
     * @param fileName Closure file name.
     * @param <T> Type of closure argument.
     * @return IgniteInClosure for post-configuration.
     * @throws IOException In case of an error.
     * @see #storeToFile(IgniteInClosure, String)
     */
    @SuppressWarnings("unchecked")
    public static <T> IgniteInClosure<T> readClosureFromFileAndDelete(String fileName) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(fileName), StandardCharsets.UTF_8)) {
            return (IgniteInClosure<T>)new XStream().fromXML(reader);
        }
        finally {
            U.delete(new File(fileName));
        }
    }
}
