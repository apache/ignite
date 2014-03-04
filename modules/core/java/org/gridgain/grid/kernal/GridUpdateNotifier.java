/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.kernal.processors.license.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jetbrains.annotations.*;
import org.w3c.dom.*;
import org.w3c.dom.Node;
import org.w3c.tidy.*;
import java.io.*;
import java.net.*;
import java.util.concurrent.*;

/**
 * This class is responsible for notification about new version availability. Note that this class
 * does not send any information and merely accesses the {@code www.gridgain.org} web site for the
 * latest version data.
 * <p>
 * Note also that this connectivity is not necessary to successfully start the system as it will
 * gracefully ignore any errors occurred during notification and verification process.
 * See {@link #HTTP_URL} for specific access URL used.
 *
 * @author @java.author
 * @version @java.version
 */
class GridUpdateNotifier {
    /*
     * *********************************************************
     * DO NOT CHANGE THIS URL OR HOW IT IS PUT IN ONE LINE.    *
     * THIS URL IS HANDLED BY POST-BUILD PROCESS AND IT HAS TO *
     * BE PLACED EXACTLY HOW IT IS SHOWING.                    *
     * *********************************************************
     */
    /** Access URL to be used to access latest version data. */
    private static final String HTTP_URL =
        /*@java.update.status.url*/"http://www.gridgain.org/update_status.php?test=vfvfvskfkeievskjv";

    /** Ant-augmented edition name. */
    private static final String EDITION = /*@java.edition*/"dev";

    /** Ant-augmented version. */
    private static final String VER = /*@java.version*/"ent-x.x.x";

    /** Throttling for logging out. */
    private static final long THROTTLE_PERIOD = 24 * 60 * 60 * 1000; // 1 day.

    /** Asynchronous checked. */
    private GridWorker checker;

    /** Latest version. */
    private volatile String latestVer;

    /** HTML parsing helper. */
    private final Tidy tidy;

    /** Grid name. */
    private final String gridName;

    /**  Whether or not to report only new version. */
    private boolean reportOnlyNew;

    /** */
    private int topSize;

    /** */
    private long lastLog = -1;

    /** */
    private GridLicenseProcessor licProc;

    /**
     * Creates new notifier with default values.
     *
     * @param gridName gridName
     * @param reportOnlyNew Whether or not to report only new version.
     */
    GridUpdateNotifier(String gridName, boolean reportOnlyNew) {
        tidy = new Tidy();

        tidy.setQuiet(true);
        tidy.setOnlyErrors(true);
        tidy.setShowWarnings(false);
        tidy.setInputEncoding("UTF8");
        tidy.setOutputEncoding("UTF8");

        this.gridName = gridName;
        this.reportOnlyNew = reportOnlyNew;
    }

    /**
     * @param reportOnlyNew Whether or not to report only new version.
     */
    void reportOnlyNew(boolean reportOnlyNew) {
        this.reportOnlyNew = reportOnlyNew;
    }

    /**
     * @param topSize Size of topology for license verification purpose.
     */
    void topologySize(int topSize) {
        this.topSize = topSize;
    }

    /**
     * @param licProc License processor.
     */
    void licenseProcessor(GridLicenseProcessor licProc) {
        this.licProc = licProc;
    }

    /**
     * @return Latest version.
     */
    String latestVersion() {
        return latestVer;
    }

    /**
     * Starts asynchronous process for retrieving latest version data from {@link #HTTP_URL}.
     *
     * @param exec Executor service.
     * @param log Logger.
     */
    void checkForNewVersion(Executor exec, GridLogger log) {
        assert log != null;

        log = log.getLogger(getClass());

        try {
            exec.execute(checker = new UpdateChecker(log));
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
    void reportStatus(GridLogger log) {
        assert log != null;

        log = log.getLogger(getClass());

        // Don't join it to avoid any delays on update checker.
        // Checker thread will eventually exit.
        U.cancel(checker);

        String latestVer = this.latestVer;

        if (latestVer != null)
            if (latestVer.equals(VER)) {
                if (!reportOnlyNew)
                    throttle(log, false, "Your version is up to date.");
            }
            else
                throttle(log, true, "New version is available at www.gridgain.com: " + latestVer);
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
    private void throttle(GridLogger log, boolean warn, String msg) {
        assert(log != null);
        assert(msg != null);

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
     * Asynchronous checker of the latest version available.
     *
     * @author @java.author
     */
    private class UpdateChecker extends GridWorker {
        /** Logger. */
        private final GridLogger log;

        /**
         * Creates checked with given logger.
         *
         * @param log Logger.
         */
        UpdateChecker(GridLogger log) {
            super(gridName, "grid-version-checker", log);

            this.log = log.getLogger(getClass());
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            try {
                GridProductLicense lic = licProc != null ? licProc.license() : null;

                URLConnection conn = new URL(HTTP_URL +
                    (HTTP_URL.endsWith(".php") ? '?' : '&') +
                    (topSize > 0 ? "t=" + topSize + "&" : "") +
                    (lic != null ? "l=" + lic.id() + "&" : "") +
                    "p=" + gridName)
                    .openConnection();

                if (!isCancelled()) {
                    // Timeout after 3 seconds.
                    conn.setConnectTimeout(3000);
                    conn.setReadTimeout(3000);

                    InputStream in = null;

                    Document dom = null;

                    // gridgain.org analyzes User-Agent header and default value "Java/1.7.0_XX" does not work.
                    conn.setRequestProperty("User-Agent", "");

                    try {
                        in = conn.getInputStream();

                        if (in == null)
                            return;

                        dom = tidy.parseDOM(in, null);
                    }
                    catch (IOException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to connect to GridGain update server. " + e.getMessage());
                    }
                    finally {
                        U.close(in, log);
                    }

                    if (dom != null)
                        latestVer = obtainVersionFrom(dom);
                }
            }
            catch (Exception ignore) {
                // Ignore this error.
            }
        }

        /**
         * Gets the version from the current {@code node}, if one exists.
         *
         * @param node W3C DOM node.
         * @return Version or {@code null} if one's not found.
         */
        @Nullable private String obtainVersionFrom(Node node) {
            assert node != null;

            if (node instanceof Element && "meta".equals(node.getNodeName().toLowerCase())) {
                Element meta = (Element)node;

                String name = meta.getAttribute("name");

                if ((EDITION + "-version").equals(name)) {
                    String content = meta.getAttribute("content");

                    if (content != null && !content.isEmpty())
                        return content;
                }
            }

            NodeList childNodes = node.getChildNodes();

            for (int i = 0; i < childNodes.getLength(); i++) {
                String ver = obtainVersionFrom(childNodes.item(i));

                if (ver != null)
                    return ver;
            }

            return null;
        }
    }
}
