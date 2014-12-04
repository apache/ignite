/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework.junits.logger;

import org.apache.ignite.logger.*;
import org.apache.log4j.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Log4J {@link org.apache.log4j.RollingFileAppender} with added support for grid node IDs.
 */
public class GridLog4jRollingFileAppender extends RollingFileAppender implements GridLoggerNodeIdAware {
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

        if (fileName != null) { // fileName could be null if GRIDGAIN_HOME is not defined.
            if (baseFileName == null)
                baseFileName = fileName;

            fileName = U.nodeIdLogFileName(nodeId, baseFileName);
        }
        else {
            String tmpDir = GridSystemProperties.getString("java.io.tmpdir");

            if (tmpDir != null) {
                baseFileName = new File(tmpDir, "gridgain.log").getAbsolutePath();

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
