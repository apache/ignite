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

import java.io.Serializable;

/**
 * TensorFlow job archive that keeps archived working directory and command to be executed.
 */
public class TensorFlowJobArchive implements Serializable {
    /** */
    private static final long serialVersionUID = -5977231383594482459L;

    /** Upstream cache name. */
    private final String upstreamCacheName;

    /** Archived working directory. */
    private final byte[] data;

    /** Command to be executed with arguments. */
    private final String[] commands;

    /**
     * Constructs a new instance of TensorFlow job archive.
     *
     * @param upstreamCacheName Upstream cache name.
     * @param data Archived working directory.
     * @param commands Command to be executed with arguments.
     */
    public TensorFlowJobArchive(String upstreamCacheName, byte[] data, String[] commands) {
        this.upstreamCacheName = upstreamCacheName;
        this.data = data;
        this.commands = commands;
    }

    /** */
    public String getUpstreamCacheName() {
        return upstreamCacheName;
    }

    /** */
    public byte[] getData() {
        return data;
    }

    /** */
    public String[] getCommands() {
        return commands;
    }
}
