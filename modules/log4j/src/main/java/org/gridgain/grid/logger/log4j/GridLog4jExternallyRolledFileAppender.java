/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.logger.log4j;

import org.apache.log4j.varia.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Log4J {@link ExternallyRolledFileAppender} with added support for grid node IDs.
 */
public class GridLog4jExternallyRolledFileAppender extends ExternallyRolledFileAppender
    implements GridLoggerNodeIdSupported {
    /** Node ID. */
    private UUID nodeId;

    /** Basic log file name. */
    private String baseFileName;

    /**
     * Default constructor (does not do anything).
     */
    public GridLog4jExternallyRolledFileAppender() {
        init();
    }

    /**
     *
     */
    private void init() {
        GridLog4jLogger.addAppender(this);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
    @Override public synchronized void setNodeId(UUID nodeId) {
        A.notNull(nodeId, "nodeId");
        A.notNull(fileName, "fileName");

        this.nodeId = nodeId;

        if (baseFileName == null)
            baseFileName = fileName;

        fileName = U.nodeIdLogFileName(nodeId, baseFileName);
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
