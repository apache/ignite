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

package org.apache.ignite.spi.deployment.uri.scanners;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.*;

import java.io.*;
import java.net.*;

/**
 * Base deployment scanner implementation. It simplifies scanner implementation
 * by providing loggers, executors and file names parsing methods.
 */
public abstract class GridUriDeploymentScanner {
    /** Grid name. */
    private final String gridName;

    /** URI that scanner should looks after. */
    @GridToStringExclude
    private final URI uri;

    /** Temporary deployment directory. */
    private final File deployDir;

    /** Scan frequency. */
    private final long freq;

    /** Found files filter. */
    private final FilenameFilter filter;

    /** Scanner listener which should be notified about changes. */
    private final GridUriDeploymentScannerListener lsnr;

    /** Logger. */
    private final IgniteLogger log;

    /** Scanner implementation. */
    private IgniteSpiThread scanner;

    /** Whether first scan completed or not. */
    private boolean firstScan = true;

    /**
     * Scans URI for new, updated or deleted files.
     */
    protected abstract void process();

    /**
     * Creates new scanner.
     *
     * @param gridName Grid name.
     * @param uri URI which scanner should looks after.
     * @param deployDir Temporary deployment directory.
     * @param freq Scan frequency.
     * @param filter Found files filter.
     * @param lsnr Scanner listener which should be notifier about changes.
     * @param log Logger.
     */
    protected GridUriDeploymentScanner(
        String gridName,
        URI uri,
        File deployDir,
        long freq,
        FilenameFilter filter,
        GridUriDeploymentScannerListener lsnr,
        IgniteLogger log) {
        assert uri != null;
        assert freq > 0;
        assert deployDir != null;
        assert filter != null;
        assert log != null;
        assert lsnr != null;

        this.gridName = gridName;
        this.uri = uri;
        this.deployDir = deployDir;
        this.freq = freq;
        this.filter = filter;
        this.log = log.getLogger(getClass());
        this.lsnr = lsnr;
    }

    /**
     * Starts scanner.
     */
    public void start() {
        scanner = new IgniteSpiThread(gridName, "grid-uri-scanner", log) {
            /** {@inheritDoc} */
            @SuppressWarnings({"BusyWait"})
            @Override protected void body() throws InterruptedException  {
                try {
                    while (!isInterrupted()) {
                        try {
                            process();
                        }
                        finally {
                            // Do it in finally to avoid any hanging.
                            if (firstScan) {
                                firstScan = false;

                                lsnr.onFirstScanFinished();
                            }
                        }

                        Thread.sleep(freq);
                    }
                }
                finally {
                    // Double check. If we were cancelled before anything has been scanned.
                    if (firstScan) {
                        firstScan = false;

                        lsnr.onFirstScanFinished();
                    }
                }
            }
        };

        scanner.start();

        if (log.isDebugEnabled())
            log.debug("Grid URI deployment scanner started: " + this);
    }

    /**
     * Cancels scanner execution.
     */
    public void cancel() {
        U.interrupt(scanner);
    }

    /**
     * Joins scanner thread.
     */
    public void join() {
        U.join(scanner, log);

        if (log.isDebugEnabled())
            log.debug("Grid URI deployment scanner stopped: " + this);
    }

    /**
     * Tests whether scanner was cancelled before or not.
     *
     * @return {@code true} if scanner was cancelled and {@code false}
     *      otherwise.
     */
    protected boolean isCancelled() {
        assert scanner != null;

        return scanner.isInterrupted();
    }

    /**
     * Creates temp file in temp directory.
     *
     * @param fileName File name.
     * @param tmpDir dir to creating file.
     * @return created file.
     * @throws IOException if error occur.
     */
    protected File createTempFile(String fileName, File tmpDir) throws IOException {
        assert fileName != null;

        int idx = fileName.lastIndexOf('.');

        if (idx == -1)
            idx = fileName.length();

        String prefix = fileName.substring(0, idx);
        if (idx < 3) { // Prefix must be at least 3 characters long. See File.createTempFile(...).
            prefix += "___";
        }

        String suffix = fileName.substring(idx);

        return File.createTempFile(prefix, suffix, tmpDir);
    }

    /**
     * Gets file URI for the given file name. It extends any given name with {@link #uri}.
     *
     * @param name File name.
     * @return URI for the given file name.
     */
    protected String getFileUri(String name) {
        assert name != null;

        String fileUri = uri.toString();

        fileUri = fileUri.length() > 0 && fileUri.charAt(fileUri.length() - 1) == '/' ? fileUri + name :
            fileUri + '/' + name;

        return fileUri;
    }

    /**
     * Tests whether first scan completed or not.
     *
     * @return {@code true} if first scan has been already completed and
     *      {@code false} otherwise.
     */
    protected boolean isFirstScan() {
        return firstScan;
    }

    /**
     * Gets deployment URI.
     *
     * @return Deployment URI.
     */
    protected final URI getUri() {
        return uri;
    }

    /**
     * Gets deployment frequency.
     *
     * @return Deployment frequency.
     */
    protected final long getFrequency() {
        return freq;
    }

    /**
     * Gets temporary deployment directory.
     *
     * @return Temporary deployment directory.
     */
    protected final File getDeployDirectory() {
        return deployDir;
    }

    /**
     * Gets filter for found files. Before {@link #lsnr} is notified about
     * changes with certain file last should be accepted by filter.
     *
     * @return New, updated or deleted file filter.
     */
    protected final FilenameFilter getFilter() {
        return filter;
    }

    /**
     * Gets deployment listener.
     *
     * @return Listener which should be notified about all deployment events
     *      by scanner.
     */
    protected final GridUriDeploymentScannerListener getListener() {
        return lsnr;
    }

    /**
     * Gets scanner logger.
     *
     * @return Logger.
     */
    protected final IgniteLogger getLogger() {
        return log;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridUriDeploymentScanner.class, this,
            "uri", uri != null ? U.hidePassword(uri.toString()) : null);
    }
}
