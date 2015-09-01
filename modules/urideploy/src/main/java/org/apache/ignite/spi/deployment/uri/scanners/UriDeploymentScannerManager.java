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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiThread;

/**
 * URI deployment scanner manager.
 */
public class UriDeploymentScannerManager implements UriDeploymentScannerContext {
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

    /** Underlying scanner. */
    private final UriDeploymentScanner scanner;

    /** Scanner implementation. */
    private IgniteSpiThread scannerThread;

    /** Whether first scan completed or not. */
    private boolean firstScan = true;

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
     * @param scanner Scanner.
     */
    public UriDeploymentScannerManager(
        String gridName,
        URI uri,
        File deployDir,
        long freq,
        FilenameFilter filter,
        GridUriDeploymentScannerListener lsnr,
        IgniteLogger log,
        UriDeploymentScanner scanner) {
        assert uri != null;
        assert freq > 0;
        assert deployDir != null;
        assert filter != null;
        assert log != null;
        assert lsnr != null;
        assert scanner != null;

        this.gridName = gridName;
        this.uri = uri;
        this.deployDir = deployDir;
        this.freq = freq;
        this.filter = filter;
        this.log = log.getLogger(getClass());
        this.lsnr = lsnr;
        this.scanner = scanner;
    }

    /**
     * Starts scanner.
     */
    public void start() {
        scannerThread = new IgniteSpiThread(gridName, "grid-uri-scanner", log) {
            /** {@inheritDoc} */
            @SuppressWarnings({"BusyWait"})
            @Override protected void body() throws InterruptedException  {
                try {
                    while (!isInterrupted()) {
                        try {
                            scanner.scan(UriDeploymentScannerManager.this);
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

        scannerThread.start();

        if (log.isDebugEnabled())
            log.debug("Grid URI deployment scanner started: " + this);
    }

    /**
     * Cancels scanner execution.
     */
    public void cancel() {
        U.interrupt(scannerThread);
    }

    /**
     * Joins scanner thread.
     */
    public void join() {
        U.join(scannerThread, log);

        if (log.isDebugEnabled())
            log.debug("Grid URI deployment scanner stopped: " + this);
    }

    /** {@inheritDoc} */
    public boolean isCancelled() {
        assert scannerThread != null;

        return scannerThread.isInterrupted();
    }

    /** {@inheritDoc} */
    public File createTempFile(String fileName, File tmpDir) throws IOException {
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

    /** {@inheritDoc} */
    public boolean isFirstScan() {
        return firstScan;
    }

    /** {@inheritDoc} */
    public URI getUri() {
        return uri;
    }

    /** {@inheritDoc} */
    public File getDeployDirectory() {
        return deployDir;
    }

    /** {@inheritDoc} */
    public FilenameFilter getFilter() {
        return filter;
    }

    /** {@inheritDoc} */
    public GridUriDeploymentScannerListener getListener() {
        return lsnr;
    }

    /** {@inheritDoc} */
    public IgniteLogger getLogger() {
        return log;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(UriDeploymentScannerManager.class, this, "uri", U.hidePassword(uri.toString()));
    }
}