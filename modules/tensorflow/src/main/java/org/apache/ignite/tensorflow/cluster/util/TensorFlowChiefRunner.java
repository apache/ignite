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

package org.apache.ignite.tensorflow.cluster.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.tensorflow.cluster.spec.TensorFlowClusterSpec;
import org.apache.ignite.tensorflow.cluster.tfrunning.TensorFlowServer;
import org.apache.ignite.tensorflow.cluster.tfrunning.TensorFlowServerScriptFormatter;
import org.apache.ignite.tensorflow.core.util.NativeProcessRunner;
import org.apache.ignite.tensorflow.core.pythonrunning.PythonProcessBuilderSupplier;
import org.apache.ignite.tensorflow.core.util.CustomizableThreadFactory;

/**
 * TensorFlow chief job runner.
 */
public class TensorFlowChiefRunner {
    /** Chief process thread name. */
    private static final String CHIEF_PROCESS_THREAD_NAME = "tensorflow-chief";

    /** TensorFlow server script formatter. */
    private static final TensorFlowServerScriptFormatter scriptFormatter = new TensorFlowServerScriptFormatter();

    /** Native process runner. */
    private static final NativeProcessRunner processRunner = new NativeProcessRunner();

    /** Process builder supplier. */
    private static final PythonProcessBuilderSupplier processBuilderSupplier = new PythonProcessBuilderSupplier(true);

    /** Logger. */
    private final IgniteLogger log;

    /** TensorFlow cluster specification. */
    private final TensorFlowClusterSpec spec;

    /** Executors that is used to start and control native process. */
    private final ExecutorService executor;

    /** Future of the chief process. */
    private Future<?> fut;

    /**
     * Constructs a new instance of TensorFlow chief runner.
     *
     * @param spec TensorFlow cluster specification.
     */
    public TensorFlowChiefRunner(TensorFlowClusterSpec spec) {
        this.log = Ignition.ignite().log().getLogger(TensorFlowChiefRunner.class);
        this.spec = spec;
        this.executor = Executors.newSingleThreadExecutor(
            new CustomizableThreadFactory(CHIEF_PROCESS_THREAD_NAME, true)
        );
    }

    /**
     * Starts chief runner process.
     */
    public void start() {
        log.info("Starting chief");

        TensorFlowServer srv = new TensorFlowServer(spec, "chief", 0);

        fut = executor.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    log.info("Starting chief native process");
                    processRunner.startAndWait(
                        processBuilderSupplier.get(),
                        scriptFormatter.format(srv, true, Ignition.ignite()),
                        System.out::println,
                        System.err::println
                    );
                    log.info("Chief native process has been completed");
                }
                catch (InterruptedException e) {
                    log.info("Chief native process has been interrupted");
                    break;
                }
                catch (Exception e) {
                    log.error("Chief native process failed", e);
                }
            }
        });
    }

    /**
     * Stop chief runner process.
     */
    public void stop() {
        log.info("Stopping chief");

        if (fut != null && !fut.isDone())
            fut.cancel(true);
    }
}
