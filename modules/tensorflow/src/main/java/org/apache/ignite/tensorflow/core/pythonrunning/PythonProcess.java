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

package org.apache.ignite.tensorflow.core.pythonrunning;

import java.io.Serializable;
import java.util.UUID;

/**
 * Python process specification.
 */
public class PythonProcess implements Serializable {
    /** */
    private static final long serialVersionUID = -1623536488451695210L;

    /** Stdin of the process. */
    private final String stdin;

    /** Node identifier. */
    private final UUID nodeId;

    /** Meta information that adds to script as arguments.  */
    private final String[] meta;

    /**
     * Constructs a new instance of python process.
     *
     * @param stdin  Stdin of the process.
     * @param nodeId Node identifier.
     * @param meta Meta information that adds to script as arguments.
     */
    public PythonProcess(String stdin, UUID nodeId, String... meta) {
        assert nodeId != null : "Node identifier should not be null";

        this.stdin = stdin;
        this.nodeId = nodeId;
        this.meta = meta;
    }

    /** */
    public String getStdin() {
        return stdin;
    }

    /** */
    public UUID getNodeId() {
        return nodeId;
    }

    /** */
    public String[] getMeta() {
        return meta;
    }
}
