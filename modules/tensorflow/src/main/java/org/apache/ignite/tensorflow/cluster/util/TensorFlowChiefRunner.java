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
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.tensorflow.cluster.spec.TensorFlowClusterSpec;
import org.apache.ignite.tensorflow.cluster.tfrunning.TensorFlowServer;
import org.apache.ignite.tensorflow.cluster.tfrunning.TensorFlowServerScriptFormatter;
import org.apache.ignite.tensorflow.core.pythonrunning.PythonProcessBuilderSupplier;
import org.apache.ignite.tensorflow.core.util.AsyncNativeProcessRunner;
import org.apache.ignite.tensorflow.core.util.NativeProcessRunner;

/**
 * Utils class that helps to start and stop chief process.
 */
public class TensorFlowChiefRunner extends AsyncNativeProcessRunner {
    /** Ignite instance. */
    private final Ignite ignite;

    /** TensorFlow cluster specification. */
    private final TensorFlowClusterSpec spec;

    /** Output stream data consumer. */
    private final Consumer<String> out;

    /** Error stream data consumer. */
    private final Consumer<String> err;

    /**
     * Constructs a new instance of TensorFlow chief runner.
     *
     * @param ignite Ignite instance.
     * @param executor Executor to be used in {@link AsyncNativeProcessRunner}.
     * @param spec TensorFlow cluster specification.
     * @param out Output stream data consumer.
     * @param err Error stream data consumer.
     */
    public TensorFlowChiefRunner(Ignite ignite, ExecutorService executor, TensorFlowClusterSpec spec,
        Consumer<String> out, Consumer<String> err) {
        super(ignite, executor);
        this.ignite = ignite;
        this.spec = spec;
        this.out = out;
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public NativeProcessRunner doBefore() {
        TensorFlowServer srv = new TensorFlowServer(spec, TensorFlowClusterResolver.CHIEF_JOB_NAME, 0);

        return new NativeProcessRunner(
            new PythonProcessBuilderSupplier(
                true,
                "job:" + srv.getJobName(),
                "task:" + srv.getTaskIdx()
            ).get(),
            new TensorFlowServerScriptFormatter().format(srv, true, ignite),
            out,
            err
        );
    }

    /** {@inheritDoc} */
    @Override public void doAfter() {
        // Do nothing.
    }
}
