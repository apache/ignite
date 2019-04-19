/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.logger.log4j;

import java.io.IOException;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.log4j.varia.ExternallyRolledFileAppender;

/**
 * Log4J {@link ExternallyRolledFileAppender} with added support for grid node IDs.
 */
public class Log4JExternallyRolledFileAppender extends ExternallyRolledFileAppender implements Log4jFileAware {
    /** Basic log file name. */
    private String baseFileName;

    /**
     * Default constructor (does not do anything).
     */
    public Log4JExternallyRolledFileAppender() {
        init();
    }

    /**
     *
     */
    private void init() {
        Log4JLogger.addAppender(this);
    }

    /** {@inheritDoc} */
    @Override public synchronized void updateFilePath(IgniteClosure<String, String> filePathClos) {
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