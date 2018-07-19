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

package org.apache.ignite.tensorflow.core.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * Utils class that helps to start native processes.
 */
public class NativeProcessRunner {
    /** Thread name to be used by threads that forward streams. */
    private static final String NATIVE_PROCESS_FORWARD_STREAM_THREAD_NAME = "tensorflow-forward-native-output";

    /**
     * Starts the native process and waits it to be completed successfully or with exception.
     *
     * @param procBuilder Process builder.
     * @param stdin Standard input of the process.
     * @param out Target for standard output stream.
     * @param err Target for error stream.
     */
    public void startAndWait(ProcessBuilder procBuilder, String stdin, Consumer<String> out, Consumer<String> err)
        throws InterruptedException {
        Process proc;
        try {
            proc = procBuilder.start();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        Thread shutdownHook = new Thread(proc::destroy);
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        Future<?> outForward = forwardStream(proc.getInputStream(), out);
        Future<?> errForward = forwardStream(proc.getErrorStream(), err);

        try {
            if (stdin != null) {
                PrintWriter writer = new PrintWriter(proc.getOutputStream());
                writer.println(stdin);
                writer.flush();
            }

            int status;
            try {
                status = proc.waitFor();
            }
            catch (InterruptedException e) {
                proc.destroy();
                throw e;
            }

            Runtime.getRuntime().removeShutdownHook(shutdownHook);

            if (status != 0)
                throw new IllegalStateException("Native process exit status is " + status);
        }
        finally {
            outForward.cancel(true);
            errForward.cancel(true);
        }
    }

    /**
     * Forwards stream.
     *
     * @param src Source stream.
     * @param dst Destination stream.
     * @return Future that allows to interrupt forwarding.
     */
    private Future<?> forwardStream(InputStream src, Consumer<String> dst) {
        return Executors
            .newSingleThreadExecutor(new CustomizableThreadFactory(NATIVE_PROCESS_FORWARD_STREAM_THREAD_NAME, true))
            .submit(() -> {
                Scanner scanner = new Scanner(src);

                while (!Thread.currentThread().isInterrupted() && scanner.hasNextLine())
                    dst.accept(scanner.nextLine());
            });
    }
}
