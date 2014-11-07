/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.logger.log4j;

import org.apache.log4j.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Log4J {@link DailyRollingFileAppender} with added support for grid node IDs.
 */
public class GridLog4jDailyRollingFileAppender extends DailyRollingFileAppender implements GridLog4jFileAware {
    /** Basic log file name. */
    private String baseFileName;

    /**
     * Default constructor (does not do anything).
     */
    public GridLog4jDailyRollingFileAppender() {
        init();
    }

    /**
     * Instantiate a FileAppender with given parameters.
     *
     * @param layout Layout.
     * @param filename File name.
     * @param datePtrn Date pattern.
     * @throws IOException If failed.
     */
    public GridLog4jDailyRollingFileAppender(Layout layout, String filename, String datePtrn) throws IOException {
        super(layout, filename, datePtrn);

        init();
    }

    /**
     *
     */
    private void init() {
        GridLog4jLogger.addAppender(this);
    }

    /** {@inheritDoc} */
    @Override public synchronized void updateFilePath(GridClosure<String, String> filePathClos) {
        A.notNull(filePathClos, "filePathClos");

        if (baseFileName == null)
            baseFileName = fileName;

        fileName = filePathClos.apply(baseFileName);
    }

    /** {@inheritDoc} */
    @Override public synchronized void setFile(String fileName, boolean fileAppend, boolean bufIO, int bufSize)
        throws IOException {
        if (baseFileName != null)
            super.setFile(fileName, fileAppend, bufIO, bufSize);
    }
}
