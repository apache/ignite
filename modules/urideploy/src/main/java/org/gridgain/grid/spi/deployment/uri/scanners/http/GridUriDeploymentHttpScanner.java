/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri.scanners.http;

import org.gridgain.grid.logger.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.deployment.uri.scanners.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;
import org.w3c.dom.*;
import org.w3c.tidy.*;

import javax.net.ssl.*;
import java.io.*;
import java.net.*;
import java.security.*;
import java.security.cert.*;
import java.util.*;

/**
 * URI deployment HTTP scanner.
 */
public class GridUriDeploymentHttpScanner extends GridUriDeploymentScanner {
    /** Secure socket protocol to use. */
    private static final String PROTOCOL = "TLS";

    /** */
    @GridToStringExclude
    private URL scanDir;

    /** Cache of found files to check if any of it has been updated. */
    private Map<String, Long> tstampCache = new HashMap<>();

    /** */
    @GridToStringExclude
    private final Tidy tidy;

    /** Outgoing data SSL socket factory. */
    private SSLSocketFactory sockFactory;

    /**
     * @param gridName Grid instance name.
     * @param uri HTTP URI.
     * @param deployDir Deployment directory.
     * @param freq Scanner frequency.
     * @param filter Filename filter.
     * @param lsnr Deployment listener.
     * @param log Logger to use.
     * @throws GridSpiException Thrown in case of any error.
     */
    public GridUriDeploymentHttpScanner(
        String gridName,
        URI uri,
        File deployDir,
        long freq,
        FilenameFilter filter,
        GridUriDeploymentScannerListener lsnr,
        GridLogger log) throws GridSpiException {
        super(gridName, uri, deployDir, freq, filter, lsnr, log);

        initialize(uri);

        tidy = new Tidy();

        tidy.setQuiet(true);
        tidy.setOnlyErrors(true);
        tidy.setShowWarnings(false);
        tidy.setInputEncoding("UTF8");
        tidy.setOutputEncoding("UTF8");
   }

    /**
     * @param uri HTTP URI.
     * @throws GridSpiException Thrown in case of any error.
     */
    private void initialize(URI uri) throws GridSpiException {
        assert "http".equals(uri.getScheme()) || "https".equals(uri.getScheme());

        try {
            scanDir = new URL(uri.getScheme(), uri.getHost(), uri.getPort(), uri.getPath());
        }
        catch (MalformedURLException e) {
            scanDir = null;

            throw new GridSpiException("Wrong value for scanned HTTP directory with URI: " + uri, e);
        }

        try {
            if ("https".equals(uri.getScheme())) {
                // Set up socket factory to do authentication.
                SSLContext ctx = SSLContext.getInstance(PROTOCOL);

                ctx.init(null, getTrustManagers(), null);

                sockFactory = ctx.getSocketFactory();
            }
        }
        catch (NoSuchAlgorithmException e) {
            throw new GridSpiException("Failed to initialize SSL context. URI: " + uri, e);
        }
        catch (KeyManagementException e) {
            throw new GridSpiException("Failed to initialize SSL context. URI:" + uri, e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void process() {
        Collection<String> foundFiles = U.newHashSet(tstampCache.size());

        long start = U.currentTimeMillis();

        processHttp(foundFiles);

        if (getLogger().isDebugEnabled())
            getLogger().debug("HTTP scanner time in ms: " + (U.currentTimeMillis() - start));

        if (!isFirstScan()) {
            Collection<String> deletedFiles = new HashSet<>(tstampCache.keySet());

            deletedFiles.removeAll(foundFiles);

            if (!deletedFiles.isEmpty()) {
                List<String> uris = new ArrayList<>();

                for (String file : deletedFiles) {
                    uris.add(getFileUri(getFileName(file)));
                }

                tstampCache.keySet().removeAll(deletedFiles);

                getListener().onDeletedFiles(uris);
            }
        }
    }

    /**
     * @param files Files to process.
     */
    private void processHttp(Collection<String> files) {
        Set<String> urls = getUrls(scanDir);

        for (String url : urls) {
            String fileName = getFileName(url);

            if (getFilter().accept(null, fileName)) {
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

                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("Discovered deployment file or directory: " +
                            U.hidePassword(url));
                    }

                    file = createTempFile(fileName, getDeployDirectory());

                    // Delete file when JVM stopped.
                    file.deleteOnExit();

                    out = new FileOutputStream(file);

                    U.copy(in, out);
                }
                catch (IOException e) {
                    if (!isCancelled()) {
                        if (X.hasCause(e, ConnectException.class)) {
                            LT.warn(getLogger(), e, "Failed to connect to HTTP server (connection refused): " +
                                U.hidePassword(url));
                        }
                        else if (X.hasCause(e, UnknownHostException.class)) {
                            LT.warn(getLogger(), e, "Failed to connect to HTTP server (host is unknown): " +
                                U.hidePassword(url));
                        }
                        else
                            U.error(getLogger(), "Failed to save file: " + fileName, e);
                    }
                }
                finally {
                    U.closeQuiet(in);
                    U.closeQuiet(out);
                }

                if (file != null && file.exists() && file.length() > 0)
                    getListener().onNewOrUpdatedFile(file, getFileUri(fileName), lastModified);
            }
        }
    }

    /**
     * @param node XML element node.
     * @param res Set of URLs in string format to populate.
     * @param baseUrl Base URL.
     */
    @SuppressWarnings( {"UnusedCatchParameter", "UnnecessaryFullyQualifiedName"})
    private void findReferences(org.w3c.dom.Node node, Set<String> res, URL baseUrl) {
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
                        U.error(getLogger(), "Skipping bad URL: " + url, e1);
                    }
                }

                if (url != null)
                    res.add(url.toString());
            }
        }

        NodeList childNodes = node.getChildNodes();

        for (int i = 0; i < childNodes.getLength(); i++)
            findReferences(childNodes.item(i), res, baseUrl);
    }

    /**
     * @param url Base URL.
     * @return Set of referenced URLs in string format.
     */
    private Set<String> getUrls(URL url) {
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
            if (!isCancelled()) {
                if (X.hasCause(e, ConnectException.class)) {
                    LT.warn(getLogger(), e, "Failed to connect to HTTP server (connection refused): " +
                        U.hidePassword(url.toString()));
                }
                else if (X.hasCause(e, UnknownHostException.class)) {
                    LT.warn(getLogger(), e, "Failed to connect to HTTP server (host is unknown): " +
                        U.hidePassword(url.toString()));
                }
                else
                    U.error(getLogger(), "Failed to get HTML page: " + U.hidePassword(url.toString()), e);
            }
        }
        finally{
            U.closeQuiet(in);
        }

        if (dom != null)
            findReferences(dom, urls, url);

        return urls;
    }

    /**
     * @param url Base URL string format.
     * @return File name extracted from {@code url} string format.
     */
    private String getFileName(String url) {
        assert url != null;

        return url.substring(url.lastIndexOf('/') + 1);
    }

    /**
     * Construct array with one trust manager which don't reject input certificates.
     *
     * @return Array with one X509TrustManager implementation of trust manager.
     */
    private TrustManager[] getTrustManagers() {
        return new TrustManager[]{
            new X509TrustManager() {
                /** {@inheritDoc} */
                @Nullable @Override public X509Certificate[] getAcceptedIssuers() { return null; }

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

                    if (getLogger().isDebugEnabled())
                        getLogger().debug(buf.toString());
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

                    if (getLogger().isDebugEnabled())
                        getLogger().debug(buf.toString());
                }
            }
        };
    }


    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridUriDeploymentHttpScanner.class, this,
            "scanDir", scanDir != null ? U.hidePassword(scanDir.toString()) : null);
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
