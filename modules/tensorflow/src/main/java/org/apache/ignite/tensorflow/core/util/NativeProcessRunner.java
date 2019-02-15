/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.tensorflow.core.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Utils class that helps to start native processes.
 */
public class NativeProcessRunner {
    /** Thread name to be used by threads that forward streams. */
    private static final String NATIVE_PROCESS_FORWARD_STREAM_THREAD_NAME = "tf-forward-native-output";

    /** Process builder. */
    private final ProcessBuilder procBuilder;

    /** Standard input of the process. */
    private final String stdin;

    /** Output stream data consumer. */
    private final Consumer<String> out;

    /** Error stream data consumer. */
    private final Consumer<String> err;

    /**
     * Constructs a new instance of native process runner.
     *
     * @param procBuilder Process builder.
     * @param stdin Standard input of the process.
     * @param out Output stream data consumer.
     * @param err Error stream data consumer.
     */
    public NativeProcessRunner(ProcessBuilder procBuilder, String stdin, Consumer<String> out, Consumer<String> err) {
        this.procBuilder = procBuilder;
        this.stdin = stdin;
        this.out = out;
        this.err = err;
    }

    /**
     * Starts the native process and waits it to be completed successfully or with exception.
     */
    public void startAndWait() throws InterruptedException {
        Process proc;
        try {
            proc = procBuilder.start();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        AtomicBoolean shutdown = new AtomicBoolean();

        Thread shutdownHook = new Thread(() -> {
            shutdown.set(true);
            proc.destroy();
        });

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

            if (!shutdown.get()) {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);

                if (status != 0)
                    throw new IllegalStateException("Native process exit [status=" + status + "]");
            }
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
