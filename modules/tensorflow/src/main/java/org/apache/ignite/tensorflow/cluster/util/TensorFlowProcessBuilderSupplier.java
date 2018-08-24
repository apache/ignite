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

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.tensorflow.core.util.PythonProcessBuilderSupplier;

/**
 * Python process builder supplier that is used to create TensorFlow worker process builder.
 */
public class TensorFlowProcessBuilderSupplier extends PythonProcessBuilderSupplier {
    /** */
    private static final long serialVersionUID = 6866243505446122897L;

    /** Prefix for worker environment variables. */
    private static final String ENV_PREFIX = "IGNITE_DATASET_";

    /** Partition of the upstream cache. */
    private final Integer part;

    /**
     * Constructs a new instance of Python process builder supplier.
     *
     * @param interactive Interactive flag (allows to used standard input to pass Python script).
     * @param part Partition index.
     * @param meta Meta information that adds to script as arguments.
     */
    public TensorFlowProcessBuilderSupplier(boolean interactive, Integer part, String... meta) {
        super(interactive, meta);
        this.part = part;
    }

    /** {@inheritDoc} */
    @Override public ProcessBuilder get() {
        ProcessBuilder pythonProcBuilder = super.get();

        Ignite ignite = Ignition.ignite();
        ClusterNode locNode = ignite.cluster().localNode();

        Integer port = locNode.attribute(ClientListenerProcessor.CLIENT_LISTENER_PORT);

        Map<String, String> env = pythonProcBuilder.environment();
        env.put(ENV_PREFIX + "HOST", "localhost");

        if (port != null)
            env.put(ENV_PREFIX + "PORT", String.valueOf(port));

        if (part != null)
            env.put(ENV_PREFIX + "PART", String.valueOf(part));

        return pythonProcBuilder;
    }
}
