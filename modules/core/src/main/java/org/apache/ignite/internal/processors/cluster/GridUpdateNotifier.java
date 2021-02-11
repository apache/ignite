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

package org.apache.ignite.internal.processors.cluster;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.jetbrains.annotations.Nullable;

/**
 * This class is responsible for notification about new version availability.
 * <p>
 * Note also that this connectivity is not necessary to successfully start the system as it will
 * gracefully ignore any errors occurred during notification and verification process.
 */
class GridUpdateNotifier {
    /** Default encoding. */
    private static final String CHARSET = "UTF-8";

    /** Version comparator. */
    private static final VersionComparator VER_COMPARATOR = new VersionComparator();

    /** Throttling for logging out. */
    private static final long THROTTLE_PERIOD = 24 * 60 * 60 * 1000; // 1 day.

    /** Sleep milliseconds time for worker thread. */
    private static final int WORKER_THREAD_SLEEP_TIME = 5000;

    /** Default URL for request Ignite updates. */
    private static final String DEFAULT_IGNITE_UPDATES_URL = "https://ignite.apache.org/latest";

    /** Default HTTP parameter for request Ignite updates. */
    private static final String DEFAULT_IGNITE_UPDATES_PARAMS = "?ver=";

    /** Grid version. */
    private final String ver;

    /** Error during obtaining data. */
    private volatile Exception err;

    /** Latest version. */
    private volatile String latestVer;

    /** Download url for latest version. */
    private volatile String downloadUrl;

    /** Ignite instance name. */
    private final String igniteInstanceName;

    /** Whether or not to report only new version. */
    private volatile boolean reportOnlyNew;

    /** */
    private long lastLog = -1;

    /** Command for worker thread. */
    private final AtomicReference<Runnable> cmd = new AtomicReference<>();

    /** Worker thread to process http request. */
    private final Thread workerThread;

    /** Http client for getting Ignite updates */
    private final HttpIgniteUpdatesChecker updatesChecker;

    /**
     * Creates new notifier with default values.
     *
     * @param igniteInstanceName igniteInstanceName
     * @param ver Compound Ignite version.
     * @param reportOnlyNew Whether or not to report only new version.
     * @param updatesChecker Service for getting Ignite updates
     * @throws IgniteCheckedException If failed.
     */
    GridUpdateNotifier(
        String igniteInstanceName,
        String ver,
        boolean reportOnlyNew,
        HttpIgniteUpdatesChecker updatesChecker
    ) throws IgniteCheckedException {
        this.ver = ver;
        this.igniteInstanceName = igniteInstanceName == null ? "null" : igniteInstanceName;
        this.updatesChecker = updatesChecker;
        this.reportOnlyNew = reportOnlyNew;

        workerThread = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        Runnable cmd0 = cmd.getAndSet(null);

                        if (cmd0 != null)
                            cmd0.run();
                        else
                            Thread.sleep(WORKER_THREAD_SLEEP_TIME);
                    }
                }
                catch (InterruptedException ignore) {
                    // No-op.
                }
            }
        }, "upd-ver-checker");

        workerThread.setDaemon(true);

        workerThread.start();
    }

    /**
     * Creates new notifier with default Ignite updates URL
     */
    GridUpdateNotifier(String igniteInstanceName, String ver, boolean reportOnlyNew) throws IgniteCheckedException {
        this(igniteInstanceName, ver, reportOnlyNew,
            new HttpIgniteUpdatesChecker(DEFAULT_IGNITE_UPDATES_URL, DEFAULT_IGNITE_UPDATES_PARAMS + ver, CHARSET));
    }

    /**
     * @param reportOnlyNew Whether or not to report only new version.
     */
    void reportOnlyNew(boolean reportOnlyNew) {
        this.reportOnlyNew = reportOnlyNew;
    }

    /**
     * @return Latest version.
     */
    String latestVersion() {
        return latestVer;
    }

    /**
     * @return Error.
     */
    Exception error() {
        return err;
    }

    /**
     * Starts asynchronous process for retrieving latest version data.
     *
     * @param log Logger.
     * @param first First checking.
     */
    void checkForNewVersion(IgniteLogger log, boolean first) {
        assert log != null;

        log = log.getLogger(getClass());

        try {
            cmd.set(new UpdateChecker(log, first));
        }
        catch (RejectedExecutionException e) {
            U.error(log, "Failed to schedule a thread due to execution rejection (safely ignoring): " +
                e.getMessage());
        }
    }

    /**
     * Logs out latest version notification if such was received and available.
     *
     * @param log Logger.
     */
    void reportStatus(IgniteLogger log) {
        assert log != null;

        log = log.getLogger(getClass());

        String latestVer = this.latestVer;
        String downloadUrl = this.downloadUrl;

        downloadUrl = downloadUrl != null ? downloadUrl : IgniteKernal.SITE;

        if (latestVer != null) {
            int cmp = VER_COMPARATOR.compare(latestVer, ver);

            if (cmp == 0) {
                if (!reportOnlyNew)
                    throttle(log, false, "Your version is up to date.");
            }
            else if (cmp > 0)
                throttle(log, true, "New version is available at " + downloadUrl + ": " + latestVer);
        }
        else
            if (!reportOnlyNew)
                throttle(log, false, "Update status is not available.");
    }

    /**
     *
     * @param log Logger to use.
     * @param warn Whether or not this is a warning.
     * @param msg Message to log.
     */
    private void throttle(IgniteLogger log, boolean warn, String msg) {
        assert (log != null);
        assert (msg != null);

        long now = U.currentTimeMillis();

        if (now - lastLog > THROTTLE_PERIOD) {
            if (!warn)
                U.log(log, msg);
            else {
                U.quiet(true, msg);

                if (log.isInfoEnabled())
                    log.warning(msg);
            }

            lastLog = now;
        }
    }

    /**
     * Stops update notifier.
     */
    public void stop() {
        workerThread.interrupt();
    }

    /**
     * Asynchronous checker of the latest version available.
     */
    private class UpdateChecker extends GridWorker {
        /** Logger. */
        private final IgniteLogger log;

        /** First. */
        private final boolean first;

        /**
         * Creates checked with given logger.
         *
         * @param log Logger.
         */
        UpdateChecker(IgniteLogger log, boolean first) {
            super(igniteInstanceName, "grid-version-checker", log);

            this.log = log.getLogger(getClass());
            this.first = first;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            try {
                if (!isCancelled()) {
                    try {
                        String updatesRes = updatesChecker.getUpdates(first);

                        String[] lines = updatesRes.split("\n");

                        for (String line : lines) {
                            if (line.contains("version"))
                                latestVer = obtainVersionFrom(line);
                            else if (line.contains("downloadUrl"))
                                downloadUrl = obtainDownloadUrlFrom(line);
                        }

                        err = null;
                    }
                    catch (IOException e) {
                        err = e;

                        if (log.isDebugEnabled())
                            log.debug("Failed to connect to Ignite update server. " + e.getMessage());
                    }
                }
            }
            catch (Exception e) {
                err = e;

                if (log.isDebugEnabled())
                    log.debug("Unexpected exception in update checker. " + e.getMessage());
            }
        }

        /**
         * Gets the version from the current {@code node}, if one exists.
         *
         * @param  line Line which contains value for extract.
         * @param  metaName Name for extract.
         * @return Version or {@code null} if one's not found.
         */
        @Nullable private String obtainMeta(String metaName, String line) {
            assert line.contains(metaName);

            return line.substring(line.indexOf(metaName) + metaName.length()).trim();

        }

        /**
         * Gets the version from the current {@code node}, if one exists.
         *
         * @param  line Line which contains value for extract.
         * @return Version or {@code null} if one's not found.
         */
        @Nullable private String obtainVersionFrom(String line) {
            return obtainMeta("version=", line);
        }

        /**
         * Gets the download url from the current {@code node}, if one exists.
         *
         * @param line Which contains value for extract.
         * @return download url or {@code null} if one's not found.
         */
        @Nullable private String obtainDownloadUrlFrom(String line) {
            return obtainMeta("downloadUrl=", line);
        }
    }

    /**
     * Ignite version comparator.
     */
    private static final class VersionComparator implements Comparator<String> {
        /** Dot pattern. */
        private static final String DOT_PATTERN = "\\.";

        /** Dash pattern. */
        private static final String DASH_PATTERN = "-";

        /** {@inheritDoc} */
        @Override public int compare(String o1, String o2) {
            if (o1.equals(o2))
                return 0;

            String[] ver1 = o1.split(DOT_PATTERN, 3);
            String[] ver2 = o2.split(DOT_PATTERN, 3);

            assert ver1.length == 3;
            assert ver2.length == 3;

            if (Integer.valueOf(ver1[0]) >= Integer.valueOf(ver2[0]) &&
                Integer.valueOf(ver1[1]) >= Integer.valueOf(ver2[1]) &&
                Integer.valueOf(ver1[2].split(DASH_PATTERN)[0]) >= Integer.valueOf(ver2[2].split(DASH_PATTERN)[0]))

                return 1;
            else
                return -1;
        }
    }
}
