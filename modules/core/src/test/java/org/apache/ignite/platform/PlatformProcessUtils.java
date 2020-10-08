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

package org.apache.ignite.platform;

import org.apache.ignite.internal.util.IgniteStopwatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.time.Duration;

/**
 * Platform process utils for tests.
 */
public class PlatformProcessUtils {
    /** */
    private static final Duration MAX_WAIT = Duration.ofMinutes(3);

    /** */
    private static volatile Process process;

    /**
     * Starts a new process.
     *
     * @param file Executable name.
     * @param arg Argument.
     * @param workDir Work directory.
     * @param waitForOutput A string to look for in the output.
     */
    public static void startProcess(String file, String arg, String workDir, String waitForOutput) throws Exception {
        if (process != null)
            throw new Exception("PlatformProcessUtils can't start more than one process at a time.");

        ProcessBuilder pb = new ProcessBuilder(file, arg);
        pb.directory(new File(workDir));
        process = pb.start();

        if (waitForOutput != null) {
            InputStreamReader isr = new InputStreamReader(process.getInputStream());
            BufferedReader br = new BufferedReader(isr);
            IgniteStopwatch sw = IgniteStopwatch.createStarted();

            String line;
            while ((line = br.readLine()) != null && sw.elapsed().compareTo(MAX_WAIT) < 0) {
                System.out.println("PlatformProcessUtils >> " + line);

                if (line.contains(waitForOutput))
                    return;
            }

            process.destroyForcibly();
            process = null;

            throw new Exception("Failed to wait for specified output: '" + waitForOutput + "'");
        }
    }

    /**
     * Kills the process previously started with {@link #startProcess}.
     */
    public static void destroyProcess() throws Exception {
        if (process == null)
            throw new Exception("Process has not been started");

        process.destroy();
        process = null;
    }
}
