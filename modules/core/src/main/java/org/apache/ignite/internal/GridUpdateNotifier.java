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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.plugin.PluginProvider;
import org.jetbrains.annotations.Nullable;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;

import static java.net.URLEncoder.encode;

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

    /** Asynchronous checked. */
    private GridWorker checker;

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

            url = "http://tiny.cc/updater/update_status_ignite.php";

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
     * @param exec Executor service.
     * @param log Logger.
     */
    void checkForNewVersion(Executor exec, IgniteLogger log) {
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
    void reportStatus(IgniteLogger log) {
        assert log != null;

        log = log.getLogger(getClass());

        // Don't join it to avoid any delays on update checker.
        // Checker thread will eventually exit.
        U.cancel(checker);

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

                    conn.setConnectTimeout(3000);
                    conn.setReadTimeout(3000);

                    Document dom = null;

                    try {
                        try (OutputStream os = conn.getOutputStream()) {
                            os.write(postParams.getBytes(CHARSET));
                        }

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
}