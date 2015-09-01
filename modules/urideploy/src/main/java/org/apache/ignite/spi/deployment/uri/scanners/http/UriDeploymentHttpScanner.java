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

package org.apache.ignite.spi.deployment.uri.scanners.http;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.deployment.uri.scanners.UriDeploymentScanner;
import org.apache.ignite.spi.deployment.uri.scanners.UriDeploymentScannerContext;
import org.jetbrains.annotations.Nullable;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.tidy.Tidy;

/**
 * URI deployment HTTP scanner.
 */
public class UriDeploymentHttpScanner implements UriDeploymentScanner {
    /** Default scan frequency. */
    private static final int DFLT_SCAN_FREQ = 300000;

    /** Secure socket protocol to use. */
    private static final String PROTOCOL = "TLS";

    /** Per-URI contexts. */
    private final ConcurrentHashMap<URI, URIContext> uriCtxs = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public boolean acceptsURI(URI uri) {
        String proto = uri.getScheme().toLowerCase();

        return "http".equals(proto) || "https".equals(proto);
    }

    /** {@inheritDoc} */
    @Override public void scan(UriDeploymentScannerContext scanCtx) {
        URI uri = scanCtx.getUri();

        URIContext uriCtx = uriCtxs.get(uri);

        if (uriCtx == null) {
            uriCtx = createUriContext(uri, scanCtx);

            URIContext oldUriCtx = uriCtxs.putIfAbsent(uri, uriCtx);

            if (oldUriCtx != null)
                uriCtx = oldUriCtx;
        }

        uriCtx.scan(scanCtx);
    }

    /**
     * Create context for the given URI.
     *
     * @param uri URI.
     * @param scanCtx Scanner context.
     * @return URI context.
     */
    private URIContext createUriContext(URI uri, final UriDeploymentScannerContext scanCtx) {
        assert "http".equals(uri.getScheme()) || "https".equals(uri.getScheme());

        URL scanDir;

        try {
            scanDir = new URL(uri.getScheme(), uri.getHost(), uri.getPort(), uri.getPath());
        }
        catch (MalformedURLException e) {
            throw new IgniteSpiException("Wrong value for scanned HTTP directory with URI: " + uri, e);
        }

        SSLSocketFactory sockFactory = null;

        try {
            if ("https".equals(uri.getScheme())) {
                // Set up socket factory to do authentication.
                SSLContext ctx = SSLContext.getInstance(PROTOCOL);

                ctx.init(null, getTrustManagers(scanCtx), null);

                sockFactory = ctx.getSocketFactory();
            }
        }
        catch (NoSuchAlgorithmException e) {
            throw new IgniteSpiException("Failed to initialize SSL context. URI: " + uri, e);
        }
        catch (KeyManagementException e) {
            throw new IgniteSpiException("Failed to initialize SSL context. URI:" + uri, e);
        }

        return new URIContext(scanDir, sockFactory);
    }

    /** {@inheritDoc} */
    @Override public long getDefaultScanFrequency() {
        return DFLT_SCAN_FREQ;
    }

    /**
     * Construct array with one trust manager which don't reject input certificates.
     *
     * @param scanCtx context.
     * @return Array with one X509TrustManager implementation of trust manager.
     */
    private static TrustManager[] getTrustManagers(final UriDeploymentScannerContext scanCtx) {
        return new TrustManager[]{
            new X509TrustManager() {
                /** {@inheritDoc} */
                @Nullable
                @Override public X509Certificate[] getAcceptedIssuers() { return null; }

                /** {@inheritDoc} */
                @Override public void checkClientTrusted(X509Certificate[] certs, String authType) {
                    StringBuilder buf = new StringBuilder();

                    buf.append("Trust manager handle client certificates [authType=");
                    buf.append(authType);
                    buf.append(", certificates=");

                    for (X509Certificate cert : certs) {
                        buf.append("{type=");
                        buf.append(cert.getType());
                        buf.append(", principalName=");
                        buf.append(cert.getSubjectX500Principal().getName());
                        buf.append('}');
                    }

                    buf.append(']');

                    if (scanCtx.getLogger().isDebugEnabled())
                        scanCtx.getLogger().debug(buf.toString());
                }

                /** {@inheritDoc} */
                @Override public void checkServerTrusted(X509Certificate[] certs, String authType) {
                    StringBuilder buf = new StringBuilder();

                    buf.append("Trust manager handle server certificates [authType=");
                    buf.append(authType);
                    buf.append(", certificates=");

                    for (X509Certificate cert : certs) {
                        buf.append("{type=");
                        buf.append(cert.getType());
                        buf.append(", principalName=");
                        buf.append(cert.getSubjectX500Principal().getName());
                        buf.append('}');
                    }

                    buf.append(']');

                    if (scanCtx.getLogger().isDebugEnabled())
                        scanCtx.getLogger().debug(buf.toString());
                }
            }
        };
    }

    /**
     * URI context.
     */
    private class URIContext {
        /** */
        private final URL scanDir;

        /** Outgoing data SSL socket factory. */
        private final SSLSocketFactory sockFactory;

        /** */
        private final Tidy tidy;

        /** Cache of found files to check if any of it has been updated. */
        private final Map<String, Long> tstampCache = new HashMap<>();

        /**
         * Constructor.
         *
         * @param scanDir Scan directory.
         * @param sockFactory Socket factory.
         */
        public URIContext(URL scanDir, SSLSocketFactory sockFactory) {
            this.scanDir = scanDir;
            this.sockFactory = sockFactory;

            tidy = new Tidy();

            tidy.setQuiet(true);
            tidy.setOnlyErrors(true);
            tidy.setShowWarnings(false);
            tidy.setInputEncoding("UTF8");
            tidy.setOutputEncoding("UTF8");
        }

        /**
         * Perform scan.
         *
         * @param scanCtx Scan context.
         */
        private void scan(UriDeploymentScannerContext scanCtx) {
            Collection<String> foundFiles = U.newHashSet(tstampCache.size());

            long start = U.currentTimeMillis();

            processHttp(foundFiles, scanCtx);

            if (scanCtx.getLogger().isDebugEnabled())
                scanCtx.getLogger().debug("HTTP scanner time in ms: " + (U.currentTimeMillis() - start));

            if (!scanCtx.isFirstScan()) {
                Collection<String> deletedFiles = new HashSet<>(tstampCache.keySet());

                deletedFiles.removeAll(foundFiles);

                if (!deletedFiles.isEmpty()) {
                    List<String> uris = new ArrayList<>();

                    for (String file : deletedFiles)
                        uris.add(getFileUri(fileName(file), scanCtx));

                    tstampCache.keySet().removeAll(deletedFiles);

                    scanCtx.getListener().onDeletedFiles(uris);
                }
            }
        }

        /**
         * @param files Files to process.
         * @param scanCtx Scan context.
         */
        @SuppressWarnings("unchecked")
        private void processHttp(Collection<String> files, UriDeploymentScannerContext scanCtx) {
            Set<String> urls = getUrls(scanDir, scanCtx);

            for (String url : urls) {
                String fileName = fileName(url);

                if (scanCtx.getFilter().accept(null, fileName)) {
                    files.add(url);

                    Long lastModified = tstampCache.get(url);

                    InputStream in = null;
                    OutputStream out = null;

                    File file = null;

                    try {
                        URLConnection conn = new URL(url).openConnection();

                        if (conn instanceof HttpsURLConnection) {
                            HttpsURLConnection httpsConn = (HttpsURLConnection)conn;

                            httpsConn.setHostnameVerifier(new DeploymentHostnameVerifier());

                            assert sockFactory != null;

                            // Initialize socket factory.
                            httpsConn.setSSLSocketFactory(sockFactory);
                        }

                        if (lastModified != null)
                            conn.setIfModifiedSince(lastModified);

                        in = conn.getInputStream();

                        long rcvLastModified = conn.getLastModified();

                        if (in == null || lastModified != null && (lastModified == rcvLastModified ||
                            conn instanceof HttpURLConnection &&
                                ((HttpURLConnection)conn).getResponseCode() == HttpURLConnection.HTTP_NOT_MODIFIED))
                            continue;

                        tstampCache.put(url, rcvLastModified);

                        lastModified = rcvLastModified;

                        if (scanCtx.getLogger().isDebugEnabled()) {
                            scanCtx.getLogger().debug("Discovered deployment file or directory: " +
                                U.hidePassword(url));
                        }

                        file = scanCtx.createTempFile(fileName, scanCtx.getDeployDirectory());

                        // Delete file when JVM stopped.
                        file.deleteOnExit();

                        out = new FileOutputStream(file);

                        U.copy(in, out);
                    }
                    catch (IOException e) {
                        if (!scanCtx.isCancelled()) {
                            if (X.hasCause(e, ConnectException.class)) {
                                LT.warn(scanCtx.getLogger(), e, "Failed to connect to HTTP server " +
                                    "(connection refused): " + U.hidePassword(url));
                            }
                            else if (X.hasCause(e, UnknownHostException.class)) {
                                LT.warn(scanCtx.getLogger(), e, "Failed to connect to HTTP server " +
                                    "(host is unknown): " + U.hidePassword(url));
                            }
                            else
                                U.error(scanCtx.getLogger(), "Failed to save file: " + fileName, e);
                        }
                    }
                    finally {
                        U.closeQuiet(in);
                        U.closeQuiet(out);
                    }

                    if (file != null && file.exists() && file.length() > 0)
                        scanCtx.getListener().onNewOrUpdatedFile(file, getFileUri(fileName, scanCtx), lastModified);
                }
            }
        }

        /**
         * @param url Base URL.
         * @param scanCtx Scan context.
         * @return Set of referenced URLs in string format.
         */
        @SuppressWarnings("unchecked")
        private Set<String> getUrls(URL url, UriDeploymentScannerContext scanCtx) {
            assert url != null;

            InputStream in = null;

            Set<String> urls = new HashSet<>();

            Document dom = null;

            try {
                URLConnection conn = url.openConnection();

                if (conn instanceof HttpsURLConnection) {
                    HttpsURLConnection httpsConn = (HttpsURLConnection)conn;

                    httpsConn.setHostnameVerifier(new DeploymentHostnameVerifier());

                    assert sockFactory != null;

                    // Initialize socket factory.
                    httpsConn.setSSLSocketFactory(sockFactory);
                }

                in = conn.getInputStream();

                if (in == null)
                    throw new IOException("Failed to open connection: " + U.hidePassword(url.toString()));

                dom = tidy.parseDOM(in, null);
            }
            catch (IOException e) {
                if (!scanCtx.isCancelled()) {
                    if (X.hasCause(e, ConnectException.class)) {
                        LT.warn(scanCtx.getLogger(), e, "Failed to connect to HTTP server (connection refused): " +
                            U.hidePassword(url.toString()));
                    }
                    else if (X.hasCause(e, UnknownHostException.class)) {
                        LT.warn(scanCtx.getLogger(), e, "Failed to connect to HTTP server (host is unknown): " +
                            U.hidePassword(url.toString()));
                    }
                    else
                        U.error(scanCtx.getLogger(), "Failed to get HTML page: " + U.hidePassword(url.toString()), e);
                }
            }
            finally{
                U.closeQuiet(in);
            }

            if (dom != null)
                findReferences(dom, urls, url, scanCtx);

            return urls;
        }

        /**
         * @param node XML element node.
         * @param res Set of URLs in string format to populate.
         * @param baseUrl Base URL.
         * @param scanCtx Scan context.
         */
        @SuppressWarnings( {"UnusedCatchParameter", "UnnecessaryFullyQualifiedName"})
        private void findReferences(org.w3c.dom.Node node, Set<String> res, URL baseUrl,
            UriDeploymentScannerContext scanCtx) {
            if (node instanceof Element && "a".equals(node.getNodeName().toLowerCase())) {
                Element element = (Element)node;

                String href = element.getAttribute("href");

                if (href != null && !href.isEmpty()) {
                    URL url = null;

                    try {
                        url = new URL(href);
                    }
                    catch (MalformedURLException e) {
                        try {
                            url = new URL(baseUrl.getProtocol(), baseUrl.getHost(), baseUrl.getPort(),
                                href.charAt(0) == '/' ? href : baseUrl.getFile() + '/' + href);
                        }
                        catch (MalformedURLException e1) {
                            U.error(scanCtx.getLogger(), "Skipping bad URL: " + href, e1);
                        }
                    }

                    if (url != null)
                        res.add(url.toString());
                }
            }

            NodeList childNodes = node.getChildNodes();

            for (int i = 0; i < childNodes.getLength(); i++)
                findReferences(childNodes.item(i), res, baseUrl, scanCtx);
        }

        /**
         * @param url Base URL string format.
         * @return File name extracted from {@code url} string format.
         */
        private String fileName(String url) {
            assert url != null;

            return url.substring(url.lastIndexOf('/') + 1);
        }

        /**
         * Gets file URI for the given file name. It extends any given name with {@code URI}.
         *
         * @param name File name.
         * @param scanCtx Scan context.
         * @return URI for the given file name.
         */
        private String getFileUri(String name, UriDeploymentScannerContext scanCtx) {
            assert name != null;

            String fileUri = scanCtx.getUri().toString();

            fileUri = fileUri.length() > 0 && fileUri.charAt(fileUri.length() - 1) == '/' ? fileUri + name :
                fileUri + '/' + name;

            return fileUri;
        }
    }

    /**
     * Verifier always return successful result for any host.
     */
    private static class DeploymentHostnameVerifier implements HostnameVerifier {
        /** {@inheritDoc} */
        @Override public boolean verify(String hostname, SSLSession ses) {
            // Remote host trusted by default.
            return true;
        }
    }
}