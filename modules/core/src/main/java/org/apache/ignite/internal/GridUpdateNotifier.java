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

package org.apache.ignite.internal;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.worker.*;
import org.apache.ignite.plugin.*;
import org.jetbrains.annotations.*;
import org.w3c.dom.*;
import org.xml.sax.*;

import javax.xml.parsers.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.net.URLEncoder.*;

/**
 * This class is responsible for notification about new version availability.
 * <p>
 * Note also that this connectivity is not necessary to successfully start the system as it will
 * gracefully ignore any errors occurred during notification and verification process.
 */
class GridUpdateNotifier {
    /** Access URL to be used to access latest version data. */
    private static final String UPD_STATUS_PARAMS = IgniteProperties.get("ignite.update.status.params");

    /** Throttling for logging out. */
    private static final long THROTTLE_PERIOD = 24 * 60 * 60 * 1000; // 1 day.

    /** Grid version. */
    private final String ver;

    /** Site. */
    private final String url;

    /** Latest version. */
    private volatile String latestVer;

    /** Download url for latest version. */
    private volatile String downloadUrl;

    /** HTML parsing helper. */
    private final DocumentBuilder documentBuilder;

    /** Grid name. */
    private final String gridName;

    /** Whether or not to report only new version. */
    private volatile boolean reportOnlyNew;

    /** */
    private volatile int topSize;

    /** System properties */
    private final String vmProps;

    private final Map<String, String> pluginVers;

    /** Kernal gateway */
    private final GridKernalGateway gw;

    /** */
    private long lastLog = -1;

    /** */
    private final SingleDemonThreadExecutor verCheckerExec = new SingleDemonThreadExecutor("upd-ver-checker", 1000);

    /**
     * Creates new notifier with default values.
     *
     * @param gridName gridName
     * @param ver Compound Ignite version.
     * @param gw Kernal gateway.
     * @param pluginProviders Kernal gateway.
     * @param reportOnlyNew Whether or not to report only new version.
     * @throws IgniteCheckedException If failed.
     */
    GridUpdateNotifier(String gridName, String ver, GridKernalGateway gw, Collection<PluginProvider> pluginProviders,
        boolean reportOnlyNew) throws IgniteCheckedException {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

            documentBuilder = factory.newDocumentBuilder();

            documentBuilder.setEntityResolver(new EntityResolver() {
                @Override public InputSource resolveEntity(String publicId, String sysId) {
                    if (sysId.endsWith(".dtd"))
                        return new InputSource(new StringReader(""));

                    return null;
                }
            });

            this.ver = ver;

            //TODO: it should be  url = "http://tiny.cc/updater/update_status_ignite.php";
            url = "http://191.191.191.191/updater/update_status_ignite.php";

            this.gridName = gridName == null ? "null" : gridName;
            this.gw = gw;

            pluginVers = U.newHashMap(pluginProviders.size());

            for (PluginProvider provider : pluginProviders)
                pluginVers.put(provider.name() + "-plugin-version", provider.version());

            this.reportOnlyNew = reportOnlyNew;

            vmProps = getSystemProperties();
        }
        catch (ParserConfigurationException e) {
            throw new IgniteCheckedException("Failed to create xml parser.", e);
        }
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
                System.getProperties().store(new PrintWriter(sw), "");
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
     * @param topSize Size of topology for license verification purpose.
     */
    void topologySize(int topSize) {
        this.topSize = topSize;
    }

    /**
     * @return Latest version.
     */
    String latestVersion() {
        return latestVer;
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
            verCheckerExec.execute(new UpdateChecker(log));
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

        // Checker thread will eventually exit.
        verCheckerExec.cancelCurrentCommandIfNeeded();

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
     */
    private class UpdateChecker extends GridWorker {
        /** Default encoding. */
        private static final String CHARSET = "UTF-8";

        /** Logger. */
        private final IgniteLogger log;

        /**
         * Creates checked with given logger.
         *
         * @param log Logger.
         */
        UpdateChecker(IgniteLogger log) {
            super(gridName, "grid-version-checker", log);

            this.log = log.getLogger(getClass());
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException {
            try {
                String stackTrace = gw != null ? gw.userStackTrace() : null;

                SB plugins = new SB();

                for (Map.Entry<String, String> p : pluginVers.entrySet())
                    plugins.a("&").a(p.getKey()).a("=").a(encode(p.getValue(), CHARSET));

                String postParams =
                    "gridName=" + encode(gridName, CHARSET) +
                    (!F.isEmpty(UPD_STATUS_PARAMS) ? "&" + UPD_STATUS_PARAMS : "") +
                    (topSize > 0 ? "&topSize=" + topSize : "") +
                    (!F.isEmpty(stackTrace) ? "&stackTrace=" + encode(stackTrace, CHARSET) : "") +
                    (!F.isEmpty(vmProps) ? "&vmProps=" + encode(vmProps, CHARSET) : "") +
                    plugins.toString();

                URLConnection conn = new URL(url).openConnection();

                if (!isCancelled()) {
                    conn.setDoOutput(true);
                    conn.setRequestProperty("Accept-Charset", CHARSET);
                    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=" + CHARSET);

//                    TODO it should be next
//                    conn.setConnectTimeout(3000);
//                    conn.setReadTimeout(3000);
                    conn.setConnectTimeout(30000);
                    conn.setReadTimeout(30000);

                    Document dom = null;

                    try {
                        U.debug("Start connecting");
                        try (OutputStream os = conn.getOutputStream()) {
                            os.write(postParams.getBytes(CHARSET));
                        }
                        U.debug("Connected");

                        try (InputStream in = conn.getInputStream()) {
                            if (in == null)
                                return;

                            BufferedReader reader = new BufferedReader(new InputStreamReader(in, CHARSET));

                            StringBuilder xml = new StringBuilder();

                            String line;

                            while ((line = reader.readLine()) != null) {
                                if (line.contains("<meta") && !line.contains("/>"))
                                    line = line.replace(">", "/>");

                                xml.append(line).append('\n');
                            }

                            dom = documentBuilder.parse(new ByteArrayInputStream(xml.toString().getBytes(CHARSET)));
                        }
                    }
                    catch (IOException e) {
                        U.debug("Exception 1");
                        e.printStackTrace();

                        if (log.isDebugEnabled())
                            log.debug("Failed to connect to Ignite update server. " + e.getMessage());
                    }

                    if (dom != null) {
                        latestVer = obtainVersionFrom(dom);

                        downloadUrl = obtainDownloadUrlFrom(dom);
                    }
                }
            }
            catch (Exception e) {
                U.debug("Exception 2");
                e.printStackTrace();

                if (log.isDebugEnabled())
                    log.debug("Unexpected exception in update checker. " + e.getMessage());
            }
        }

        /**
         * Gets the version from the current {@code node}, if one exists.
         *
         * @param node W3C DOM node.
         * @return Version or {@code null} if one's not found.
         */
        @Nullable private String obtainMeta(String metaName, Node node) {
            assert node != null;

            if (node instanceof Element && "meta".equals(node.getNodeName().toLowerCase())) {
                Element meta = (Element)node;

                String name = meta.getAttribute("name");

                if (metaName.equals(name)) {
                    String content = meta.getAttribute("content");

                    if (content != null && !content.isEmpty())
                        return content;
                }
            }

            NodeList childNodes = node.getChildNodes();

            for (int i = 0; i < childNodes.getLength(); i++) {
                String ver = obtainMeta(metaName, childNodes.item(i));

                if (ver != null)
                    return ver;
            }

            return null;
        }

        /**
         * Gets the version from the current {@code node}, if one exists.
         *
         * @param node W3C DOM node.
         * @return Version or {@code null} if one's not found.
         */
        @Nullable private String obtainVersionFrom(Node node) {
            return obtainMeta("version", node);
        }

        /**
         * Gets the download url from the current {@code node}, if one exists.
         *
         * @param node W3C DOM node.
         * @return download url or {@code null} if one's not found.
         */
        @Nullable private String obtainDownloadUrlFrom(Node node) {
            return obtainMeta("downloadUrl", node);
        }
    }

    /**
     * Executor implementation which use only one demon thread for processing.
     * The executor will execute only last given command, so there is no guarantee that all commands will be processed.
     */
    private static class SingleDemonThreadExecutor implements Executor {
        /** Command to be executed. */
        private final AtomicReference<Runnable> cmd = new AtomicReference<>();

        /** Worker thread. */
        private volatile Thread workerThread;

        /** Worker is busy flag. */
        private volatile boolean workerIsBusy;

        /** Sleep mls. */
        private final long sleepMls;

        /** Worker thread name. */
        private final String name;

        /**
         * @param name Demon thread name.
         * @param sleepMls Sleep mls.
         */
        SingleDemonThreadExecutor(String name, final long sleepMls) {
            this.name = name;
            this.sleepMls = sleepMls;

            startWorkerThread();
        }

        /**
         * Starts worker thread.
         */
        private void startWorkerThread() {
            workerIsBusy = false;

            workerThread = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        while(!Thread.currentThread().isInterrupted()) {
                            Runnable cmd0 = cmd.get();

                            if (cmd0 != null) {
                                try {
                                    workerIsBusy = true;

                                    cmd0.run();

                                    cmd.compareAndSet(cmd0, null); // Forget executed task.
                                }
                                finally {
                                    workerIsBusy = false;
                                }
                            }
                            else
                                Thread.sleep(sleepMls);
                        }
                    }
                    catch (RuntimeException ignore) {
                        // Restart worker thread.
                        startWorkerThread();
                    }
                    catch (InterruptedException ignore) {
                        Thread.currentThread().interrupt();
                    }
                }
            }, name);

            workerThread.setDaemon(true);

            workerThread.start();
        }

        /** {@inheritDoc} */
        @Override public void execute(@NotNull Runnable cmd) {
            this.cmd.set(cmd);

            assert workerThread != null;
            assert workerThread.isAlive();
            assert !workerThread.isInterrupted();
        }

        /**
         * Cancels current task if needed.
         * If worker thread executes command, when this thread will be interrapted and
         */
        void cancelCurrentCommandIfNeeded() {
            if (workerIsBusy) {
                Thread workerThread0 = workerThread;

                // Order is important.
                startWorkerThread();

                workerThread0.interrupt();
            }
        }
    }
}
