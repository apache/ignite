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

package org.apache.ignite.tensorflow.core.nativerunning;

import java.io.Serializable;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Native process specification.
 */
public class NativeProcess implements Serializable {
    /** */
    private static final long serialVersionUID = -7056800139746134956L;

    /** Process builder supplier. */
    private final Supplier<ProcessBuilder> procBuilderSupplier;

    /** Stdin of the process. */
    private final String stdin;

    /** Node identifier. */
    private final UUID nodeId;

    /**
     * Constructs a new instance of native process specification.
     *
     * @param procBuilderSupplier Process builder supplier.
     * @param stdin Stdin of the process.
     * @param nodeId Node identifier.
     */
    public <T extends Supplier<ProcessBuilder> & Serializable> NativeProcess(T procBuilderSupplier, String stdin,
        UUID nodeId) {
        assert procBuilderSupplier != null : "Process builder supplier should not be null";
        assert nodeId != null : "Node identifier should not be null";

        this.procBuilderSupplier = procBuilderSupplier;
        this.stdin = stdin;
        this.nodeId = nodeId;
    }

    /** */
    public Supplier<ProcessBuilder> getProcBuilderSupplier() {
        return procBuilderSupplier;
    }

    /** */
    public String getStdin() {
        return stdin;
    }

    /** */
    public UUID getNodeId() {
        return nodeId;
    }
}
