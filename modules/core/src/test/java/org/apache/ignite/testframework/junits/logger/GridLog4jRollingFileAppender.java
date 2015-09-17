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

package org.apache.ignite.testframework.junits.logger;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.LoggerNodeIdAware;
import org.apache.log4j.Layout;
import org.apache.log4j.RollingFileAppender;

/**
 * Log4J {@link org.apache.log4j.RollingFileAppender} with added support for grid node IDs.
 */
public class GridLog4jRollingFileAppender extends RollingFileAppender implements LoggerNodeIdAware {
    /** Node ID. */
    private UUID nodeId;

    /** Basic log file name. */
    private String baseFileName;

    /**
     * Default constructor (does not do anything).
     */
    public GridLog4jRollingFileAppender() {
        init();
    }

    /**
     * Instantiate a FileAppender with given parameters.
     *
     * @param layout Layout.
     * @param filename File name.
     * @throws java.io.IOException If failed.
     */
    public GridLog4jRollingFileAppender(Layout layout, String filename) throws IOException {
        super(layout, filename);

        init();
    }

    /**
     * Instantiate a FileAppender with given parameters.
     *
     * @param layout Layout.
     * @param filename File name.
     * @param append Append flag.
     * @throws java.io.IOException If failed.
     */
    public GridLog4jRollingFileAppender(Layout layout, String filename, boolean append) throws IOException {
        super(layout, filename, append);

        init();
    }

    /**
     * Initializes appender.
     */
    private void init() {
        GridTestLog4jLogger.addAppender(this);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
    @Override public synchronized void setNodeId(UUID nodeId) {
        A.notNull(nodeId, "nodeId");

        this.nodeId = nodeId;

        if (fileName != null) { // fileName could be null if IGNITE_HOME is not defined.
            if (baseFileName == null)
                baseFileName = fileName;

            fileName = U.nodeIdLogFileName(nodeId, baseFileName);
        }
        else {
            String tmpDir = IgniteSystemProperties.getString("java.io.tmpdir");

            if (tmpDir != null) {
                baseFileName = new File(tmpDir, "ignite.log").getAbsolutePath();

                fileName = U.nodeIdLogFileName(nodeId, baseFileName);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized UUID getNodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Override public synchronized void setFile(String fileName, boolean fileAppend, boolean bufIO, int bufSize)
        throws IOException {
        if (nodeId != null)
            super.setFile(fileName, fileAppend, bufIO, bufSize);
    }
}