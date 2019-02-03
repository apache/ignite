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

import org.apache.ignite.tensorflow.cluster.spec.TensorFlowClusterSpec;
import java.io.Serializable;

/**
 * TensorFlow server specification.
 */
public class TensorFlowServer implements Serializable {
    /** */
    private static final long serialVersionUID = 165988934166176805L;

    /** TensorFlow cluster specification. */
    private final TensorFlowClusterSpec clusterSpec;

    /** Job name. */
    private final String jobName;

    /** Task index. */
    private final Integer taskIdx;

    /** Protocol. */
    private final String proto;

    /**
     * Constructs a new instance of TensorFlow server specification.
     *
     * @param clusterSpec TensorFlow cluster specification.
     * @param jobName Job name.
     */
    public TensorFlowServer(TensorFlowClusterSpec clusterSpec, String jobName) {
        this(clusterSpec, jobName, null);
    }

    /**
     * Constructs a new instance of TensorFlow server specification.
     *
     * @param clusterSpec TensorFlow cluster specification.
     * @param jobName Job name.
     * @param taskIdx Task index.
     */
    public TensorFlowServer(TensorFlowClusterSpec clusterSpec, String jobName, Integer taskIdx) {
        this(clusterSpec, jobName, taskIdx, null);
    }

    /**
     * Constructs a new instance of TensorFlow server specification.
     *
     * @param clusterSpec TensorFlow cluster specification.
     * @param jobName Job name.
     * @param taskIdx Task index.
     * @param proto Task index.
     */
    public TensorFlowServer(TensorFlowClusterSpec clusterSpec, String jobName, Integer taskIdx, String proto) {
        assert clusterSpec != null : "TensorFlow cluster specification should not be null";
        assert jobName != null : "Job name should not be null";

        this.clusterSpec = clusterSpec;
        this.jobName = jobName;
        this.taskIdx = taskIdx;
        this.proto = proto;
    }

    /** */
    public TensorFlowClusterSpec getClusterSpec() {
        return clusterSpec;
    }

    /** */
    public String getJobName() {
        return jobName;
    }

    /** */
    public Integer getTaskIdx() {
        return taskIdx;
    }

    /** */
    public String getProto() {
        return proto;
    }
}
