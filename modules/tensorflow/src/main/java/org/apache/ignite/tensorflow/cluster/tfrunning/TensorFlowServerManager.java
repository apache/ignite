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

package org.apache.ignite.tensorflow.cluster.tfrunning;

import java.io.Serializable;
import java.util.function.Supplier;
import org.apache.ignite.tensorflow.core.ProcessManager;
import org.apache.ignite.tensorflow.core.ProcessManagerWrapper;
import org.apache.ignite.tensorflow.core.pythonrunning.PythonProcess;
import org.apache.ignite.tensorflow.core.pythonrunning.PythonProcessManager;
import org.apache.ignite.tensorflow.cluster.spec.TensorFlowClusterSpec;
import org.apache.ignite.tensorflow.cluster.spec.TensorFlowServerAddressSpec;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

/**
 * TensorFlow server manager that allows to start, stop and make other actions with TensorFlow servers.
 */
public class TensorFlowServerManager extends ProcessManagerWrapper<PythonProcess, TensorFlowServer> {
    /** */
    private static final long serialVersionUID = 8355019934723445973L;

    /**
     * Constructs a new instance of TensorFlow server manager.
     *
     * @param igniteSupplier Ignite instance supplier.
     * @param <T> Type of serializable supplier.
     */
    public <T extends Supplier<Ignite> & Serializable> TensorFlowServerManager(T igniteSupplier) {
        this(new PythonProcessManager(igniteSupplier));
    }

    /**
     * Constructs a new instance of TensorFlow server manager.
     *
     * @param delegate Delegate.
     */
    public TensorFlowServerManager(ProcessManager<PythonProcess> delegate) {
        super(delegate);
    }

    /** {@inheritDoc} */
    @Override protected PythonProcess transformSpecification(TensorFlowServer spec) {
        return new PythonProcess(
            formatPythonScript(spec),
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

    /**
     * Formats TensorFlow server specification so that it's available to be passed into а python script.
     *
     * @param spec TensorFlow server specification.
     * @return Formatted TensorFlow server specification.
     */
    private String formatPythonScript(TensorFlowServer spec) {
        StringBuilder builder = new StringBuilder();

        builder.append("import tensorflow as tf").append('\n');
        builder.append("cluster = tf.train.ClusterSpec(")
            .append(formatClusterSpec(spec.getClusterSpec()))
            .append(')')
            .append('\n');
        builder.append("server = tf.train.Server(cluster");

        if (spec.getJobName() != null)
            builder.append(", job_name=\"").append(spec.getJobName()).append('"');

        if (spec.getTaskIdx() != null)
            builder.append(", task_index=").append(spec.getTaskIdx());

        if (spec.getProto() != null)
            builder.append(", protocol=\"").append(spec.getProto()).append('"');

        builder.append(')').append('\n');
        builder.append("server.join()").append('\n');

        return builder.toString();
    }

    /**
     * Formats TensorFlow cluster specification so that it's available to be passed into а python script.
     *
     * @param spec TensorFlow cluster specification.
     * @return Formatted TensorFlow cluster specification.
     */
    public String formatClusterSpec(TensorFlowClusterSpec spec) {
        StringBuilder builder = new StringBuilder();

        builder.append("{\n");

        for (Map.Entry<String, List<TensorFlowServerAddressSpec>> entry : spec.getJobs().entrySet()) {
            builder
                .append("\t\"")
                .append(entry.getKey())
                .append("\" : [ ");

            for (TensorFlowServerAddressSpec address : entry.getValue()) {
                builder
                    .append("\n\t\t\"")
                    .append(formatAddressSpec(address))
                    .append("\", ");
            }

            if (!entry.getValue().isEmpty())
                builder.delete(builder.length() - 2, builder.length());

            builder.append("\n\t],\n");
        }

        if (!spec.getJobs().isEmpty())
            builder.delete(builder.length() - 2, builder.length() - 1);

        builder.append('}');

        return builder.toString();
    }

    /**
     * Formats TensorFlow server address specification so that it's available to be passed into а python script.
     *
     * @param spec TensorFlow server address specification.
     * @return Formatted TensorFlow server address specification.
     */
    private String formatAddressSpec(TensorFlowServerAddressSpec spec) {
        UUID nodeId = spec.getNodeId();

        Ignite ignite = Ignition.localIgnite();
        Collection<String> names = ignite.cluster().forNodeId(nodeId).hostNames();

        return names.iterator().next() + ":" + spec.getPort();
    }
}
