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

package org.apache.ignite.logger.log4j;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Closure that generates file path adding node id to filename as a suffix.
 */
class IgniteLog4jNodeIdFilePath implements IgniteClosure<String, String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node id. */
    private final UUID nodeId;

    /**
     * Creates new instance.
     *
     * @param id Node id.
     */
    IgniteLog4jNodeIdFilePath(UUID id) {
        nodeId = id;
    }

    /** {@inheritDoc} */
    @Override public String apply(String oldPath) {
        if (!F.isEmpty(U.GRIDGAIN_LOG_DIR))
            return U.nodeIdLogFileName(nodeId, new File(U.GRIDGAIN_LOG_DIR, "gridgain.log").getAbsolutePath());

        if (oldPath != null) // fileName could be null if GRIDGAIN_HOME is not defined.
            return U.nodeIdLogFileName(nodeId, oldPath);

        String tmpDir = IgniteSystemProperties.getString("java.io.tmpdir");

        if (tmpDir != null)
            return U.nodeIdLogFileName(nodeId, new File(tmpDir, "gridgain.log").getAbsolutePath());

        System.err.println("Failed to get tmp directory for log file.");

        return null;
    }
}
