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

package org.apache.ignite.tensorflow.cluster.tfrunning;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.tensorflow.cluster.spec.TensorFlowClusterSpec;
import org.apache.ignite.tensorflow.cluster.spec.TensorFlowServerAddressSpec;
import org.apache.ignite.tensorflow.cluster.util.TensorFlowProcessBuilderSupplier;
import org.apache.ignite.tensorflow.core.ProcessManager;
import org.apache.ignite.tensorflow.core.ProcessManagerWrapper;
import org.apache.ignite.tensorflow.core.nativerunning.NativeProcess;
import org.apache.ignite.tensorflow.core.nativerunning.NativeProcessManager;

/**
 * TensorFlow server manager that allows to start, stop and make other actions with TensorFlow servers.
 */
public class TensorFlowServerManager extends ProcessManagerWrapper<NativeProcess, TensorFlowServer> {
    /** TensorFlow server script formatter. */
    private static final TensorFlowServerScriptFormatter scriptFormatter = new TensorFlowServerScriptFormatter();

    /**
     * Constructs a new instance of TensorFlow server manager.
     *
     * @param ignite Ignite instance.
     */
    public TensorFlowServerManager(Ignite ignite) {
        this(new NativeProcessManager(ignite));
    }

    /**
     * Constructs a new instance of TensorFlow server manager.
     *
     * @param delegate Delegate.
     */
    public TensorFlowServerManager(ProcessManager<NativeProcess> delegate) {
        super(delegate);
    }

    /** {@inheritDoc} */
    @Override protected NativeProcess transformSpecification(TensorFlowServer spec) {
        return new NativeProcess(
            new TensorFlowProcessBuilderSupplier(
                true,
                true,
                "job:" + spec.getJobName(),
                "task:" + spec.getTaskIdx()
            ),
            scriptFormatter.format(spec, true, Ignition.ignite()),
            getNode(spec)
        );
    }

    /**
     * Extracts the cluster node server should be running on.
     *
     * @param spec TensorFlow server specification.
     * @return Node identifier.
     */
    private UUID getNode(TensorFlowServer spec) {
        TensorFlowClusterSpec clusterSpec = spec.getClusterSpec();
        Map<String, List<TensorFlowServerAddressSpec>> jobs = clusterSpec.getJobs();
        List<TensorFlowServerAddressSpec> tasks = jobs.get(spec.getJobName());
        TensorFlowServerAddressSpec addr = tasks.get(spec.getTaskIdx());

        return addr.getNodeId();
    }
}
