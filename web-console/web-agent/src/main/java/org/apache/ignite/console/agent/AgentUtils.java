

package org.apache.ignite.console.agent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.ProtectionDomain;
import java.security.UnrecoverableKeyException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.ignite.IgniteException;
import org.apache.ignite.console.websocket.WebSocketResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpProxy;
import org.eclipse.jetty.client.Origin;
import org.eclipse.jetty.client.ProxyConfiguration;
import org.eclipse.jetty.client.Socks4Proxy;
import org.eclipse.jetty.client.util.BasicAuthentication;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;

import static java.net.Proxy.NO_PROXY;
import static java.net.Proxy.Type.SOCKS;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.console.utils.Utils.toJson;
import static org.eclipse.jetty.client.api.Authentication.ANY_REALM;

/**
 * Utility methods.
 */
public class AgentUtils {
    /** */
    private static final Logger log = LoggerFactory.getLogger(AgentUtils.class.getName());

    /** */
    public static final String[] EMPTY = {};

    /**
     * Default constructor.
     */
    private AgentUtils() {
        // No-op.
    }

    /**
     * @param path Path to normalize.
     * @return Normalized file path.
     */
    private static String normalizePath(String path) {
        return path != null ? path.replace('\\', '/') : null;
    }

    /**
     * @return App folder.
     */
    public static File getAgentHome() {
        try {
            ProtectionDomain domain = AgentLauncher.class.getProtectionDomain();

            // Should not happen, but to make sure our code is not broken.
            if (domain == null || domain.getCodeSource() == null || domain.getCodeSource().getLocation() == null) {
                log.warn("Failed to resolve application folder!");

                return null;
            }

            // Resolve path to class-file.
            URI classesUri = domain.getCodeSource().getLocation().toURI();

            boolean win = System.getProperty("os.name").toLowerCase().contains("win");

            // Overcome UNC path problem on Windows (http://www.tomergabel.com/JavaMishandlesUNCPathsOnWindows.aspx)
            if (win && classesUri.getAuthority() != null)
                classesUri = new URI(classesUri.toString().replace("file://", "file:/"));

            return new File(classesUri).getParentFile();
        }
        catch (URISyntaxException | SecurityException ignored) {
            log.warn("Failed to resolve application folder!");

            return null;
        }
    }

    /**
     * Gets file associated with path.
     * <p>
     * First check if path is relative to agent home.
     * If not, check if path is absolute.
     * If all checks fail, then {@code null} is returned.
     * <p>
     *
     * @param path Path to resolve.
     * @return Resolved path as file, or {@code null} if path cannot be resolved.
     */
    public static File resolvePath(String path) {
        assert path != null;

        File home = getAgentHome();

        if (home != null) {
            File file = new File(home, normalizePath(path));

            if (file.exists())
                return file;
        }

        // 2. Check given path as absolute.
        File file = new File(path);

        if (file.exists())
            return file;

        return null;
    }

    /**
     *
     * @param keyStore Path to key store.
     * @param keyStorePwd Optional key store password.
     * @param trustAll Whether we should trust for self-signed certificate.
     * @param trustStore Path to trust store.
     * @param trustStorePwd Optional trust store password.
     * @param ciphers Optional list of enabled cipher suites.
     * @return SSL context factory.
     */
    public static SslContextFactory sslContextFactory(
        String keyStore,
        String keyStorePwd,
        boolean trustAll,
        String trustStore,
        String trustStorePwd,
        List<String> ciphers
    ) {
        SslContextFactory sslCtxFactory = new SslContextFactory.Client();

        if (!F.isEmpty(keyStore)) {
            sslCtxFactory.setKeyStorePath(keyStore);

            if (!F.isEmpty(keyStorePwd))
                sslCtxFactory.setKeyStorePassword(keyStorePwd);
        }

        if (trustAll) {
            sslCtxFactory.setTrustAll(true);
            // Available in Jetty >= 9.4.15.x sslCtxFactory.setHostnameVerifier((hostname, session) -> true);
        }
        else if (!F.isEmpty(trustStore)) {
            sslCtxFactory.setTrustStorePath(trustStore);

            if (!F.isEmpty(trustStorePwd))
                sslCtxFactory.setTrustStorePassword(trustStorePwd);
        }

        if (!F.isEmpty(ciphers))
            sslCtxFactory.setIncludeCipherSuites(ciphers.toArray(EMPTY));

        return  sslCtxFactory;
    }

    /**
     * @param s String with sensitive data.
     * @return Secured string.
     */
    public static String secured(String s) {
        int len = s.length();
        int toShow = len > 4 ? 4 : 1;

        return new String(new char[len - toShow]).replace('\0', '*') + s.substring(len - toShow, len);
    }

    /**
     * @param c Collection with sensitive data.
     * @return Secured string.
     */
    public static String secured(Collection<String> c) {
        return c.stream().map(AgentUtils::secured).collect(Collectors.joining(", "));
    }

    /**
     * @param httpClient Http client.
     * @param proxies Proxies.
     */
    private static void addAuthentication(HttpClient httpClient, List<ProxyConfiguration.Proxy> proxies) {
        proxies.forEach(p -> {
            String user, pwd;

            if (p instanceof HttpProxy) {
                String scheme = p.getURI().getScheme();

                user = System.getProperty(scheme + ".proxyUsername");
                pwd = System.getProperty(scheme + ".proxyPassword");
            }
            else {
                user = System.getProperty("java.net.socks.username");
                pwd = System.getProperty("java.net.socks.password");
            }

            httpClient.getAuthenticationStore().addAuthentication(
                new BasicAuthentication(p.getURI(), ANY_REALM, user, pwd)
            );
        });
    }

    /**
     * @param str Server uri.
     */
    public static void configureProxy(HttpClient httpClient, String str) {
        try {
            URI uri = URI.create(str);

            URI proxyUri = new URI("ws".equalsIgnoreCase(uri.getScheme()) ? "http" : "https",
                uri.getUserInfo(),
                uri.getHost(),
                uri.getPort(),
                uri.getPath(),
                uri.getQuery(),
                uri.getFragment()
            );

            boolean secure = "https".equalsIgnoreCase(proxyUri.getScheme());

            List<ProxyConfiguration.Proxy> proxies = ProxySelector.getDefault().select(proxyUri).stream()
                .filter(p -> !p.equals(NO_PROXY))
                .map(p -> {
                    InetSocketAddress inetAddr = (InetSocketAddress)p.address();

                    Origin.Address addr = new Origin.Address(inetAddr.getHostName(), inetAddr.getPort());

                    if (p.type() == SOCKS)
                        return new Socks4Proxy(addr, secure);

                    return new HttpProxy(addr, secure);
                })
                .collect(toList());

            httpClient.getProxyConfiguration().getProxies().addAll(proxies);

            addAuthentication(httpClient, proxies);
        }
        catch (Exception e) {
            log.warn("Failed to configure proxy.", e);
        }
    }

    /**
     * @return String with short node UUIDs.
     */
    public static String nid8(Collection<UUID> nids) {
        return nids.stream().map(nid -> U.id8(nid).toUpperCase()).collect(Collectors.joining(",", "[", "]"));
    }

    /**
     * Simple entry generator.
     * 
     * @param key Key.
     * @param val Value.
     */
    public static <K, V> Map.Entry<K, V> entry(K key, V val) {
        return new AbstractMap.SimpleEntry<>(key, val);
    }

    /**
     * Collector.
     */
    public static <K, U> Collector<Map.Entry<K, U>, ?, Map<K, U>> entriesToMap() {
        return Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue);
    }

    /**
     * Send event to websocket.
     *
     * @param ses Websocket session.
     * @param evt Event.
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @throws Exception If failed to send event.
     */
    public static void send(Session ses, WebSocketResponse evt, long timeout, TimeUnit unit) throws Exception {        

        try {
        	ses.getRemote().sendString(toJson(evt));
        }
        catch (IOException e) {
            throw e;
        }
    }

    /**
     * Trim all elements in list and return new list.
     *
     * @param lst List of elements to trim.
     * @return List with trimmed values.
     */
    public static List<String> trim(List<String> lst) {
        return F.isEmpty(lst) ? lst : lst.stream().map(String::trim).collect(toList());
    }

    /**
     * @param name Name.
     * @param keyStorePath Key store path.
     * @param keyStorePwd Key store password.
     */
    public static String getPasswordFromKeyStore(String name, String keyStorePath, String keyStorePwd) {
        if (F.isEmpty(keyStorePath))
            throw new IgniteException("Empty path to key store with passwords");

        try {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            try (FileInputStream fis = new FileInputStream(keyStorePath)) {
                ks.load(fis, keyStorePwd.toCharArray());
                SecretKey secretKey = (SecretKey) ks.getKey(name, keyStorePwd.toCharArray());

                if (secretKey == null)
                    throw new IgniteException(String.format("Failed to find password in key store: [name=%s, keyStorePath=%s]", name, keyStorePath));

                return new String(secretKey.getEncoded());
            }
        }
        catch (IOException e) {
            if (e.getCause() instanceof UnrecoverableKeyException)
                throw new IgniteException("Failed to read password from key store, please check key store password", e);

            throw new IgniteException("Failed to open passwords key store: " + keyStorePath, e);
        }
        catch (GeneralSecurityException e) {
            throw new IgniteException("Failed to read password from key store", e);
        }
    }

    /**
           * 解压zip文件
     * 
     * @param zipFile目标文件
     * @param descDir解压后存放的位置
     * @return true/false
     * @throws IOException 
     */
    public static boolean unZip(File zipFile, String descDir) throws IOException {
        boolean flag = false;
        File pathFile = new File(descDir);
        if (!pathFile.exists()) {
            pathFile.mkdirs();
        }
        ZipFile zip = null;
        if(File.pathSeparatorChar=='\\') {
            // 指定编码，否则压缩包里面不能有中文目录  windows gbk
            zip = new ZipFile(zipFile, Charset.forName("gbk"));
        }	
        else {
        	zip = new ZipFile(zipFile, Charset.forName("utf-8"));
        }
        for (Enumeration<? extends ZipEntry> entries = zip.entries(); entries.hasMoreElements();) {
            ZipEntry entry = (ZipEntry) entries.nextElement();
            String zipEntryName = entry.getName();
            InputStream in = zip.getInputStream(entry);
            String outPath = (descDir + zipEntryName).replace("/",File.separator);
            // 判断路径是否存在,不存在则创建文件路径
            File file = new File(outPath.substring(0,outPath.lastIndexOf(File.separator)));
            if (!file.exists()) {
                file.mkdirs();
            }
            // 判断文件全路径是否为文件夹,如果是上面已经上传,不需要解压
            if (new File(outPath).isDirectory()) {
                continue;
            }

            OutputStream out = new FileOutputStream(outPath);
            byte[] buf1 = new byte[2048];
            int len;
            while ((len = in.read(buf1)) > 0) {
                out.write(buf1, 0, len);
            }
            in.close();
            out.close();
        }
        flag = true;
        // 必须关闭，否则无法删除该zip文件
        zip.close();
       
        return flag;
    }
}
