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

package org.apache.ignite.tensorflow.core.nativerunning.task;

import java.util.function.Supplier;
import org.apache.ignite.tensorflow.core.nativerunning.NativeProcess;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.tensorflow.core.util.CustomizableThreadFactory;

/**
 * Task that starts native process by its specification.
 */
public class NativeProcessStartTask implements IgniteRunnable {
    /** */
    private static final long serialVersionUID = 8421398298283116405L;

    /** Native process specification. */
    private final NativeProcess procSpec;

    /**
     * Constructs a new instance of native process start task.
     *
     * @param procSpec Native process specification.
     */
    public NativeProcessStartTask(NativeProcess procSpec) {
        assert procSpec != null : "Process specification should not be null";

        this.procSpec = procSpec;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        Supplier<ProcessBuilder> procBuilderSupplier = procSpec.getProcBuilderSupplier();
        ProcessBuilder procBuilder = procBuilderSupplier.get();

        Process proc;
        try {
            proc = procBuilder.start();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        Thread shutdownHook = new Thread(proc::destroy);
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        Future<?> outForward = forwardStream(proc.getInputStream(), System.out);
        Future<?> errForward = forwardStream(proc.getErrorStream(), System.err);

        try {
            if (procSpec.getStdin() != null) {
                PrintWriter writer = new PrintWriter(proc.getOutputStream());
                writer.println(procSpec.getStdin());
                writer.flush();
            }

            int status;
            try {
                status = proc.waitFor();
            }
            catch (InterruptedException e) {
                proc.destroy();
                status = proc.exitValue();
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
    private Future<?> forwardStream(InputStream src, PrintStream dst) {
        return Executors
            .newSingleThreadExecutor(new CustomizableThreadFactory("NATIVE_PROCESS_FORWARD_STREAM", true))
            .submit(() -> {
                Scanner scanner = new Scanner(src);

                while (!Thread.currentThread().isInterrupted() && scanner.hasNextLine())
                    dst.println(scanner.nextLine());
            });
    }
}
