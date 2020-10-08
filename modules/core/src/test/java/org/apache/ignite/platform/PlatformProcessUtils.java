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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.IgniteException;

/**
 * Platform process utils for tests.
 */
@SuppressWarnings("unused") // Called from Platform.
public class PlatformProcessUtils {
    /** */
    private static volatile Process process;

    /**
     * Starts a new process.
     *
     * @param file Executable name.
     * @param arg1 Argument.
     * @param arg2 Argument.
     * @param workDir Work directory.
     * @param waitForOutput A string to look for in the output.
     */
    public static void startProcess(String file, String arg1, String arg2, String workDir, String waitForOutput)
            throws Exception {
        if (process != null)
            throw new Exception("PlatformProcessUtils can't start more than one process at a time.");

        ProcessBuilder pb = new ProcessBuilder(file, arg1, arg2);
        pb.directory(new File(workDir));
        pb.redirectErrorStream(true);
        process = pb.start();

        InputStreamReader isr = new InputStreamReader(process.getInputStream());
        BufferedReader br = new BufferedReader(isr);

        if (waitForOutput != null) {
            try {
                Future<?> f = ForkJoinPool.commonPool().submit(() -> {
                    try {
                        String line;
                        while ((line = br.readLine()) != null) {
                            System.out.println("PlatformProcessUtils >> " + line);

                            if (line.contains(waitForOutput))
                                return;
                        }
                    } catch (IOException ioException) {
                        throw new IgniteException(ioException);
                    }
                });

                f.get(3, TimeUnit.MINUTES);
            } catch (Exception e) {
                process.destroyForcibly();
                process = null;

                throw new Exception("Failed to wait for specified output: '" + waitForOutput + "'", e);
            }

            if (!process.isAlive()) {
                throw new Exception("Process has exited unexpectedly: " + process.exitValue());
            }
        }

        // Read output continuously in the background.
        ForkJoinPool.commonPool().submit(() -> {
            try {
                String line;
                while ((line = br.readLine()) != null)
                    System.out.println("PlatformProcessUtils >> " + line);
            } catch (IOException ioException) {
                throw new IgniteException(ioException);
            }
        });
    }

    /**
     * Kills the process previously started with {@link #startProcess}.
     */
    public static void destroyProcess() throws Exception {
        if (process == null)
            throw new Exception("Process has not been started");

        process.destroyForcibly();
        process = null;
    }
}
