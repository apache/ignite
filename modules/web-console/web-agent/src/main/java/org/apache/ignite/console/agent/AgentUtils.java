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

package org.apache.ignite.console.agent;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.ProtectionDomain;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import io.socket.client.Ack;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import okhttp3.ConnectionSpec;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.ssl.SSLContextWrapper;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Utility methods.
 */
public class AgentUtils {
    /** */
    private static final Logger log = Logger.getLogger(AgentUtils.class.getName());

    /** */
    private static final char[] EMPTY_PWD = new char[0];

    /** JSON object mapper. */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        // Register special module with basic serializers.
        MAPPER.registerModule(new JsonOrgModule());
    }

    /** */
    private static final Ack NOOP_CB = args -> {
        if (args != null && args.length > 0 && args[0] instanceof Throwable)
            log.error("Failed to execute request on agent.", (Throwable)args[0]);
        else
            log.info("Request on agent successfully executed " + Arrays.toString(args));
    };

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
    public static String normalizePath(String path) {
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
                log.warn("Failed to resolve agent jar location!");

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
            log.warn("Failed to resolve agent jar location!");

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
     * Get callback from handler arguments.
     *
     * @param args Arguments.
     * @return Callback or noop callback.
     */
    public static Ack safeCallback(Object[] args) {
        boolean hasCb = args != null && args.length > 0 && args[args.length - 1] instanceof Ack;

        return hasCb ? (Ack)args[args.length - 1] : NOOP_CB;
    }

    /**
     * Remove callback from handler arguments.
     *
     * @param args Arguments.
     * @return Arguments without callback.
     */
    public static Object[] removeCallback(Object[] args) {
        boolean hasCb = args != null && args.length > 0 && args[args.length - 1] instanceof Ack;

        return hasCb ? Arrays.copyOf(args, args.length - 1) : args;
    }

    /**
     * Map java object to JSON object.
     *
     * @param obj Java object.
     * @return {@link JSONObject} or {@link JSONArray}.
     * @throws IllegalArgumentException If conversion fails due to incompatible type.
     */
    public static Object toJSON(Object obj) {
        if (obj instanceof Iterable)
            return MAPPER.convertValue(obj, JSONArray.class);

        return MAPPER.convertValue(obj, JSONObject.class);
    }

    /**
     * Map JSON object to java object.
     *
     * @param obj {@link JSONObject} or {@link JSONArray}.
     * @param toValType Expected value type.
     * @return Mapped object type of {@link T}.
     * @throws IllegalArgumentException If conversion fails due to incompatible type.
     */
    public static <T> T fromJSON(Object obj, Class<T> toValType) throws IllegalArgumentException {
        return MAPPER.convertValue(obj, toValType);
    }

    /**
     * @param pathToJks Path to java key store file.
     * @param pwd Key store password.
     * @return Key store.
     * @throws GeneralSecurityException If failed to load key store.
     * @throws IOException If failed to load key store file content.
     */
    private static KeyStore keyStore(String pathToJks, char[] pwd) throws GeneralSecurityException, IOException {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream(pathToJks), pwd);

        return keyStore;
    }

    /**
     * @param keyStorePath Path to key store.
     * @param keyStorePwd Key store password.
     * @return Key managers.
     * @throws GeneralSecurityException If failed to load key store.
     * @throws IOException If failed to load key store file content.
     */
    private static KeyManager[] keyManagers(String keyStorePath, String keyStorePwd)
        throws GeneralSecurityException, IOException {
        if (keyStorePath == null)
            return null;

        char[] keyPwd = keyStorePwd != null ? keyStorePwd.toCharArray() : EMPTY_PWD;

        KeyStore keyStore = keyStore(keyStorePath, keyPwd);

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyPwd);

        return kmf.getKeyManagers();
    }

    /**
     * @param trustAll {@code true} If we trust to self-signed sertificates.
     * @param trustStorePath Path to trust store file.
     * @param trustStorePwd Trust store password.
     * @return Trust manager
     * @throws GeneralSecurityException If failed to load trust store.
     * @throws IOException If failed to load trust store file content.
     */
    public static X509TrustManager trustManager(boolean trustAll, String trustStorePath, String trustStorePwd)
        throws GeneralSecurityException, IOException {
        if (trustAll)
            return disabledTrustManager();

        if (trustStorePath == null)
            return null;

        char[] trustPwd = trustStorePwd != null ? trustStorePwd.toCharArray() : EMPTY_PWD;
        KeyStore trustKeyStore = keyStore(trustStorePath, trustPwd);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        tmf.init(trustKeyStore);

        TrustManager[] trustMgrs = tmf.getTrustManagers();

        return (X509TrustManager)Arrays.stream(trustMgrs)
            .filter(tm -> tm instanceof X509TrustManager)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("X509TrustManager manager not found"));
    }

    /**
     * Create SSL socket factory.
     *
     * @param keyStorePath Path to key store.
     * @param keyStorePwd Key store password.
     * @param trustMgr Trust manager.
     * @param cipherSuites Optional cipher suites.
     * @throws GeneralSecurityException If failed to load trust store.
     * @throws IOException If failed to load store file content.
     */
    public static SSLSocketFactory sslSocketFactory(
        String keyStorePath, String keyStorePwd,
        X509TrustManager trustMgr,
        List<String> cipherSuites
    ) throws GeneralSecurityException, IOException {
        KeyManager[] keyMgrs = keyManagers(keyStorePath, keyStorePwd);

        if (keyMgrs == null && trustMgr == null)
            return null;

        SSLContext ctx = SSLContext.getInstance("TLS");

        if (!F.isEmpty(cipherSuites))
            ctx = new SSLContextWrapper(ctx, new SSLParameters(cipherSuites.toArray(new String[0])));

        ctx.init(keyMgrs, new TrustManager[] {trustMgr}, null);

        return ctx.getSocketFactory();
    }

    /**
     * Create SSL configuration.
     *
     * @param cipherSuites SSL cipher suites.
     */
    public static List<ConnectionSpec> sslConnectionSpec(List<String> cipherSuites) {
        return Collections.singletonList(new ConnectionSpec.Builder(ConnectionSpec.MODERN_TLS)
            .cipherSuites(cipherSuites.toArray(new String[0]))
            .build());
    }

    /**
     * Create a trust manager that trusts all certificates.
     */
    private static X509TrustManager disabledTrustManager() {
        return new X509TrustManager() {
            /** {@inheritDoc} */
            @Override public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }

            /** {@inheritDoc} */
            @Override public void checkClientTrusted(X509Certificate[] certs, String authType) {
                // No-op.
            }

            /** {@inheritDoc} */
            @Override public void checkServerTrusted(X509Certificate[] certs, String authType) {
                // No-op.
            }
        };
    }
}
