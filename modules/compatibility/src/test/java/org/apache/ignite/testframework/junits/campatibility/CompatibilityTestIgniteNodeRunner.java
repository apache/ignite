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

package org.apache.ignite.testframework.junits.campatibility;

import com.thoughtworks.xstream.XStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.multijvm.IgniteNodeRunner;

/**
 * Run Ignite node.
 */
public class CompatibilityTestIgniteNodeRunner extends IgniteNodeRunner {
    /** */
    private static final String IGNITE_CLOSURE_FILE = System.getProperty("java.io.tmpdir") +
        File.separator + "igniteClosure.tmp_";

    /**
     * Starts {@link Ignite} with test's default configuration.
     *
     * @param args Arguments.
     * @throws Exception In case of an error.
     */
    public static void main(String[] args) throws Exception {
        X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());

        X.println("Starting Ignite Node... Args=" + Arrays.toString(args));

        if (args.length < 1)
            throw new IllegalArgumentException("Two arguments expected: [path/to/closure/file] [node-UUID]");

        IgniteConfiguration cfg = CompatibilityTestsFacade.getConfiguration();

        cfg.setNodeId(UUID.fromString(args[1]));

        IgniteInClosure<IgniteConfiguration> clos = readClosureFromFileAndDelete(args[0]);

        clos.apply(cfg);

        Ignite ignite = Ignition.start(cfg);

        GridTestUtils.setFieldValue(new IgniteNodeRunner(), "ignite", ignite);
    }

    /**
     * Stores {@link IgniteInClosure} to file as xml.
     *
     * @param clos IgniteInClosure.
     * @return A name of file where the configuration was stored.
     * @throws IOException In case of an error.
     * @see #readClosureFromFileAndDelete(String)
     */
    public static String storeToFile(IgniteInClosure clos) throws IOException {
        String fileName = IGNITE_CLOSURE_FILE + clos.hashCode();

        storeToFile(clos, fileName);

        return fileName;
    }

    /**
     * Stores {@link IgniteInClosure} to file as xml.
     *
     * @param clos IgniteInClosure.
     * @param fileName A name of file where the closure was stored.
     * @throws IOException In case of an error.
     * @see #readClosureFromFileAndDelete(String)
     */
    public static void storeToFile(IgniteInClosure clos, String fileName) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(fileName), StandardCharsets.UTF_8)) {
            new XStream().toXML(clos, writer);
        }
    }

    /**
     * Reads closure from given file name and delete the file after.
     *
     * @param closFileName Closure file name.
     * @return IgniteInClosure for post-configuration.
     * @throws IOException In case of an error.
     * @see #storeToFile(IgniteInClosure, String)
     */
    @SuppressWarnings("unchecked")
    public static IgniteInClosure<IgniteConfiguration> readClosureFromFileAndDelete(
        String closFileName) throws IOException {
        try (BufferedReader closReader = Files.newBufferedReader(Paths.get(closFileName), StandardCharsets.UTF_8)) {
            return (IgniteInClosure)new XStream().fromXML(closReader);
        }
        finally {
            new File(closFileName).delete();
        }
    }
}
