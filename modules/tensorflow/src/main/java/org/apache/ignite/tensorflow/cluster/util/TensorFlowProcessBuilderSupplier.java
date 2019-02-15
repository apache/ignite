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
