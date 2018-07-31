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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.tensorflow.core.nativerunning.NativeProcess;
import org.apache.ignite.tensorflow.core.util.NativeProcessRunner;

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

        NativeProcessRunner procRunner = new NativeProcessRunner(
            procBuilder,
            procSpec.getStdin(),
            System.out::println,
            System.err::println
        );

        IgniteLogger log = Ignition.ignite().log().getLogger(NativeProcessStartTask.class);

        try {
            log.debug("Starting native process");
            procRunner.startAndWait();
            log.debug("Native process completed");
        }
        catch (InterruptedException e) {
            log.debug("Native process interrupted");
        }
        catch (Exception e) {
            log.error("Native process failed", e);
            throw e;
        }
    }
}
