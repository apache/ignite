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

package org.apache.ignite.internal.processors.cluster;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.GridKernalGateway;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteProperties;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.plugin.PluginProvider;
import org.jetbrains.annotations.Nullable;

import static java.net.URLEncoder.encode;

/**
 * This class is responsible for notification about new version availability.
 * <p>
 * Note also that this connectivity is not necessary to successfully start the system as it will
 * gracefully ignore any errors occurred during notification and verification process.</p>
 * <p>
 * TODO GG-14736 rework this for GridGain Community versions.</p>
 */
class GridUpdateNotifier {
    /** Community edition. */
    private static final String COMMUNITY_EDITION = "ce";

    /** Enterprise edition. */
    private static final String ENTERPRISE_EDITION = "ee";

    /** Ultimate edition. */
    private static final String ULTIMATE_EDITION = "ue";

    /** Default encoding. */
    static final String CHARSET = "UTF-8";

    /** Access URL to be used to access latest version data. */
    private final String updStatusParams = IgniteProperties.get("ignite.update.status.params");

    /** Throttling for logging out. */
    private static final long THROTTLE_PERIOD = 24 * 60 * 60 * 1000; // 1 day.

    /** Sleep milliseconds time for worker thread. */
    private static final int WORKER_THREAD_SLEEP_TIME = 5000;

    /** Default url for request GridGain updates. */
    static final String DEFAULT_GRIDGAIN_UPDATES_URL = "https://www.gridgain.com/notifier/update";

    /** Grid version. */
    private final String ver;

    /** Discovery SPI. */
    private final GridDiscoveryManager discoSpi;

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

    /** System properties. */
    private final String vmProps;

    /** Plugins information for request. */
    private final String pluginsVers;

    /** Kernal gateway. */
    private final GridKernalGateway gw;

    /** Number of server nodes in topology. */
    private int srvNodes = 0;

    /** Edition of GridGain */
    private String product;

    /** */
    private long lastLog = -1;

    /** Command for worker thread. */
    private final AtomicReference<Runnable> cmd = new AtomicReference<>();

    /** Worker thread to process http request. */
    private final Thread workerThread;

    /** Http client for getting Ignite updates. */
    private final HttpIgniteUpdatesChecker updatesChecker;

    /** Excluded VM props. */
    private static final Set<String> PROPS_TO_EXCLUDE = new HashSet<>();

    static {
        PROPS_TO_EXCLUDE.add("sun.boot.library.path");
        PROPS_TO_EXCLUDE.add("sun.boot.class.path");
        PROPS_TO_EXCLUDE.add("java.class.path");
        PROPS_TO_EXCLUDE.add("java.endorsed.dirs");
        PROPS_TO_EXCLUDE.add("java.library.path");
        PROPS_TO_EXCLUDE.add("java.home");
        PROPS_TO_EXCLUDE.add("java.ext.dirs");
        PROPS_TO_EXCLUDE.add("user.dir");
        PROPS_TO_EXCLUDE.add("user.home");
        PROPS_TO_EXCLUDE.add("user.name");
        PROPS_TO_EXCLUDE.add("IGNITE_HOME");
        PROPS_TO_EXCLUDE.add("IGNITE_CONFIG_URL");
    }

    /**
     * Creates new notifier with default values.
     *
     * @param igniteInstanceName igniteInstanceName
     * @param ver Compound Ignite version.
     * @param gw Kernal gateway.
     * @param discovery Discovery SPI.
     * @param pluginProviders Kernal gateway.
     * @param reportOnlyNew Whether or not to report only new version.
     * @param updatesChecker Service for getting Ignite updates
     * @throws IgniteCheckedException If failed.
     */
    GridUpdateNotifier(String igniteInstanceName, String ver, GridKernalGateway gw,
        GridDiscoveryManager discovery,
        Collection<PluginProvider> pluginProviders,
        boolean reportOnlyNew, HttpIgniteUpdatesChecker updatesChecker) throws IgniteCheckedException {
        try {
            this.ver = regularize(ver);
            this.igniteInstanceName = igniteInstanceName == null ? "null" : igniteInstanceName;
            this.gw = gw;
            this.updatesChecker = updatesChecker;

            SB pluginsBuilder = new SB();

            for (PluginProvider provider : pluginProviders)
                pluginsBuilder.a("&").a(provider.name() + "-plugin-version").a("=").
                    a(encode(Optional.ofNullable(provider.version()).orElse("UNKNOWN"), CHARSET));

            pluginsVers = pluginsBuilder.toString();

            this.reportOnlyNew = reportOnlyNew;

            vmProps = getSystemProperties();

            product = checkProduct();

            discoSpi = discovery;

            workerThread = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        while (!Thread.currentThread().isInterrupted()) {
                            Runnable cmd0 = cmd.getAndSet(null);

                            if (cmd0 != null)
                                cmd0.run();
                            else
                                //noinspection BusyWait
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
        catch (UnsupportedEncodingException e) {
            throw new IgniteCheckedException("Failed to encode.", e);
        }
    }

    /**
     * Check product version.
     *
     * @return CE, EE or UE
     */
    private String checkProduct() {
        String res = COMMUNITY_EDITION;

        try {
            Class c = Class.forName("org.gridgain.grid.internal.processors.cache.database.SnapshotsMessageFactory");
            res = ULTIMATE_EDITION;
        }
        catch (ClassNotFoundException e) {
            try {
                Class c = Class.forName("org.gridgain.grid.persistentstore.GridSnapshot");
                res = ENTERPRISE_EDITION;
            }
            catch (ClassNotFoundException e1) {
                // NO-OP.
            }
        }

        return res;
    }

    /**
     * Regularize version. Version 8.7.3-pXXX should be equal to 8.7.3-pYYY
     *
     * @param ver Version.
     * @return Regularized version.
     */
    private String regularize(String ver) {
        int pos = ver.indexOf('-');
        return ver.substring(0, pos >= 0 ? pos : ver.length());
    }

    /**
     * Gets system properties.
     *
     * @return System properties.
     */
    private static String getSystemProperties() {
        try {
            StringWriter sw = new StringWriter();

            try {
                Properties snapshot = IgniteSystemProperties.snapshot();

                for (String toExclude : PROPS_TO_EXCLUDE)
                    snapshot.remove(toExclude);

                snapshot.store(new PrintWriter(sw), "");
            }
            catch (IOException ignore) {
                return null;
            }

            return sw.toString();
        }
        catch (SecurityException ignore) {
            return null;
        }
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
     */
    void checkForNewVersion(IgniteLogger log) {
        assert log != null;

        log = log.getLogger(getClass());

        try {
            cmd.set(new UpdateChecker(log));
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

        if (latestVer != null)
            if (latestVer.equals(ver)) {
                if (!reportOnlyNew)
                    throttle(log, false, "Your version is up to date.");
            }
            else
                throttle(log, true, "New version is available at " + downloadUrl + ": " + latestVer);
        else if (!reportOnlyNew)
            throttle(log, false, "Update status is not available.");
    }

    /**
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

        /**
         * Creates checked with given logger.
         *
         * @param log Logger.
         */
        UpdateChecker(IgniteLogger log) {
            super(igniteInstanceName, "grid-version-checker", log);

            this.log = log.getLogger(getClass());
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            try {
                String stackTrace = gw != null ? gw.userStackTrace() : null;

                srvNodes = discoSpi.serverNodes(discoSpi.topologyVersionEx()).size();

                String postParams =
                    "igniteInstanceName=" + encode(igniteInstanceName, CHARSET) +
                        (!F.isEmpty(updStatusParams) ? "&" + updStatusParams : "") +
                        "&srvNodes=" + srvNodes +
                        "&product=" + product +
                        (!F.isEmpty(stackTrace) ? "&stackTrace=" + encode(stackTrace, CHARSET) : "") +
                        (!F.isEmpty(vmProps) ? "&vmProps=" + encode(vmProps, CHARSET) : "") +
                        pluginsVers;

                if (!isCancelled()) {
                    try {
                        String updatesRes = updatesChecker.getUpdates(postParams);

                        String[] lines = updatesRes.split("\n");

                        for (String line : lines) {
                            if (line.contains("version"))
                                latestVer = regularize(obtainVersionFrom(line));
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
         * @param line Line which contains value for extract.
         * @param metaName Name for extract.
         * @return Version or {@code null} if one's not found.
         */
        @Nullable private String obtainMeta(String metaName, String line) {
            assert line.contains(metaName);

            return line.substring(line.indexOf(metaName) + metaName.length()).trim();

        }

        /**
         * Gets the version from the current {@code node}, if one exists.
         *
         * @param line Line which contains value for extract.
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
}
