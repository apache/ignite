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

package org.apache.ignite.tensorflow.cluster;

import org.apache.ignite.tensorflow.cluster.spec.TensorFlowClusterSpec;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * TensorFlow Cluster metadata.
 */
public class TensorFlowCluster implements Serializable {
    /** */
    private static final long serialVersionUID = -6636339457255751011L;

    /** TensorFlow cluster specification. */
    private final TensorFlowClusterSpec spec;

    /** Process identifiers. */
    private final Map<UUID, List<UUID>> processes;

    /**
     * Constructs a new instance of TensorFlow cluster.
     *
     * @param spec TensorFlow cluster specification.
     * @param processes Process identifiers.
     */
    public TensorFlowCluster(TensorFlowClusterSpec spec, Map<UUID, List<UUID>> processes) {
        assert spec != null : "TensorFlow cluster specification should not be null";
        assert processes != null : "Processes should not be null";

        this.spec = spec;
        this.processes = processes;
    }

    /** */
    public TensorFlowClusterSpec getSpec() {
        return spec;
    }

    /** */
    public Map<UUID, List<UUID>> getProcesses() {
        return processes;
    }
}
