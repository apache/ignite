/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.security;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import javax.net.ServerSocketFactory;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.h2.api.ErrorCode;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.store.fs.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.StringUtils;

/**
 * A factory to create new block cipher objects.
 */
public class CipherFactory {

    /**
     * The default password to use for the .h2.keystore file
     */
    public static final String KEYSTORE_PASSWORD =
            "h2pass";

    /**
     * The security property which can prevent anonymous TLS connections.
     * Introduced into Java 6, 7, 8 in updates from July 2015.
     */
    public static final String LEGACY_ALGORITHMS_SECURITY_KEY =
            "jdk.tls.legacyAlgorithms";

    /**
     * The value of {@value #LEGACY_ALGORITHMS_SECURITY_KEY} security
     * property at the time of class initialization.
     * Null if it is not set.
     */
    public static final String DEFAULT_LEGACY_ALGORITHMS = getLegacyAlgorithmsSilently();

    private static final String KEYSTORE =
            "~/.h2.keystore";
    private static final String KEYSTORE_KEY =
            "javax.net.ssl.keyStore";
    private static final String KEYSTORE_PASSWORD_KEY =
            "javax.net.ssl.keyStorePassword";


    private CipherFactory() {
        // utility class
    }

    /**
     * Get a new block cipher object for the given algorithm.
     *
     * @param algorithm the algorithm
     * @return a new cipher object
     */
    public static BlockCipher getBlockCipher(String algorithm) {
        if ("XTEA".equalsIgnoreCase(algorithm)) {
            return new XTEA();
        } else if ("AES".equalsIgnoreCase(algorithm)) {
            return new AES();
        } else if ("FOG".equalsIgnoreCase(algorithm)) {
            return new Fog();
        }
        throw DbException.get(ErrorCode.UNSUPPORTED_CIPHER, algorithm);
    }

    /**
     * Create a secure client socket that is connected to the given address and
     * port.
     *
     * @param address the address to connect to
     * @param port the port
     * @return the socket
     */
    public static Socket createSocket(InetAddress address, int port)
            throws IOException {
        Socket socket = null;
        setKeystore();
        SSLSocketFactory f = (SSLSocketFactory) SSLSocketFactory.getDefault();
        SSLSocket secureSocket = (SSLSocket) f.createSocket();
        secureSocket.connect(new InetSocketAddress(address, port),
                SysProperties.SOCKET_CONNECT_TIMEOUT);
        secureSocket.setEnabledProtocols(
                disableSSL(secureSocket.getEnabledProtocols()));
        if (SysProperties.ENABLE_ANONYMOUS_TLS) {
            String[] list = enableAnonymous(
                    secureSocket.getEnabledCipherSuites(),
                    secureSocket.getSupportedCipherSuites());
            secureSocket.setEnabledCipherSuites(list);
        }
        socket = secureSocket;
        return socket;
    }

/**
     * Create a secure server socket. If a bind address is specified, the
     * socket is only bound to this address.
     * If h2.enableAnonymousTLS is true, an attempt is made to modify
     * the security property jdk.tls.legacyAlgorithms (in newer JVMs) to allow
     * anonymous TLS. This system change is effectively permanent for the
     * lifetime of the JVM.
     * @see #removeAnonFromLegacyAlgorithms()
     *
     * @param port the port to listen on
     * @param bindAddress the address to bind to, or null to bind to all
     *            addresses
     * @return the server socket
     */
    public static ServerSocket createServerSocket(int port,
            InetAddress bindAddress) throws IOException {
        ServerSocket socket = null;
        if (SysProperties.ENABLE_ANONYMOUS_TLS) {
            removeAnonFromLegacyAlgorithms();
        }
        setKeystore();
        ServerSocketFactory f = SSLServerSocketFactory.getDefault();
        SSLServerSocket secureSocket;
        if (bindAddress == null) {
            secureSocket = (SSLServerSocket) f.createServerSocket(port);
        } else {
            secureSocket = (SSLServerSocket) f.createServerSocket(port, 0, bindAddress);
        }
        secureSocket.setEnabledProtocols(
                disableSSL(secureSocket.getEnabledProtocols()));
        if (SysProperties.ENABLE_ANONYMOUS_TLS) {
            String[] list = enableAnonymous(
                    secureSocket.getEnabledCipherSuites(),
                    secureSocket.getSupportedCipherSuites());
            secureSocket.setEnabledCipherSuites(list);
        }

        socket = secureSocket;
        return socket;
    }

    /**
     * Removes DH_anon and ECDH_anon from a comma separated list of ciphers.
     * Only the first occurrence is removed.
     * If there is nothing to remove, returns the reference to the argument.
     * @param list  a list of names separated by commas (and spaces)
     * @return  a new string without DH_anon and ECDH_anon items,
     *          or the original if none were found
     */
    public static String removeDhAnonFromCommaSeparatedList(String list) {
        if (list == null) {
            return list;
        }
        List<String> algorithms = new LinkedList<>(Arrays.asList(list.split("\\s*,\\s*")));
        boolean dhAnonRemoved = algorithms.remove("DH_anon");
        boolean ecdhAnonRemoved = algorithms.remove("ECDH_anon");
        if (dhAnonRemoved || ecdhAnonRemoved) {
            String string = Arrays.toString(algorithms.toArray(new String[algorithms.size()]));
            return (!algorithms.isEmpty()) ? string.substring(1, string.length() - 1): "";
        }
        return list;
    }

    /**
     * Attempts to weaken the security properties to allow anonymous TLS.
     * New JREs would not choose an anonymous cipher suite in a TLS handshake
     * if server-side security property
     * {@value #LEGACY_ALGORITHMS_SECURITY_KEY}
     * were not modified from the default value.
     * <p>
     * NOTE: In current (as of 2016) default implementations of JSSE which use
     * this security property, the value is permanently cached inside the
     * ServerHandshake class upon its first use.
     * Therefore the modification accomplished by this method has to be done
     * before the first use of a server SSL socket.
     * Later changes to this property will not have any effect on server socket
     * behavior.
     */
    public static synchronized void removeAnonFromLegacyAlgorithms() {
        String legacyOriginal = getLegacyAlgorithmsSilently();
        if (legacyOriginal == null) {
            return;
        }
        String legacyNew = removeDhAnonFromCommaSeparatedList(legacyOriginal);
        if (!legacyOriginal.equals(legacyNew)) {
            setLegacyAlgorithmsSilently(legacyNew);
        }
    }

    /**
     * Attempts to resets the security property to the default value.
     * The default value of {@value #LEGACY_ALGORITHMS_SECURITY_KEY} was
     * obtained at time of class initialization.
     * <p>
     * NOTE: Resetting the property might not have any effect on server
     * socket behavior.
     * @see #removeAnonFromLegacyAlgorithms()
     */
    public static synchronized void resetDefaultLegacyAlgorithms() {
        setLegacyAlgorithmsSilently(DEFAULT_LEGACY_ALGORITHMS);
    }

    /**
     * Returns the security property {@value #LEGACY_ALGORITHMS_SECURITY_KEY}.
     * Ignores security exceptions.
     *
     * @return  the value of the security property, or null if not set
     *          or not accessible
     */
    public static String getLegacyAlgorithmsSilently() {
        String defaultLegacyAlgorithms = null;
        try {
            defaultLegacyAlgorithms = Security.getProperty(LEGACY_ALGORITHMS_SECURITY_KEY);
        } catch (SecurityException e) {
            // ignore
        }
        return defaultLegacyAlgorithms;
    }

    private static void setLegacyAlgorithmsSilently(String legacyAlgorithms) {
        if (legacyAlgorithms == null) {
            return;
        }
        try {
            Security.setProperty(LEGACY_ALGORITHMS_SECURITY_KEY, legacyAlgorithms);
        } catch (SecurityException e) {
            // ignore
        }
    }

    private static byte[] getKeyStoreBytes(KeyStore store, String password)
            throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try {
            store.store(bout, password.toCharArray());
        } catch (Exception e) {
            throw DbException.convertToIOException(e);
        }
        return bout.toByteArray();
    }

    /**
     * Get the keystore object using the given password.
     *
     * @param password the keystore password
     * @return the keystore
     */
    public static KeyStore getKeyStore(String password) throws IOException {
        try {
            // The following source code can be re-generated
            // if you have a keystore file.
            // This code is (hopefully) more Java version independent
            // than using keystores directly. See also:
            // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4887561
            // (1.4.2 cannot read keystore written with 1.4.1)
            // --- generated code start ---

            KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());

            store.load(null, password.toCharArray());
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            store.load(null, password.toCharArray());
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(
                    StringUtils.convertHexToBytes(
                            "30820277020100300d06092a864886f70d010101" +
                            "0500048202613082025d02010002818100dc0a13" +
                            "c602b7141110eade2f051b54777b060d0f74e6a1" +
                            "10f9cce81159f271ebc88d8e8aa1f743b505fc2e" +
                            "7dfe38d33b8d3f64d1b363d1af4d877833897954" +
                            "cbaec2fa384c22a415498cf306bb07ac09b76b00" +
                            "1cd68bf77ea0a628f5101959cf2993a9c23dbee7" +
                            "9b19305977f8715ae78d023471194cc900b231ee" +
                            "cb0aaea98d02030100010281810099aa4ff4d0a0" +
                            "9a5af0bd953cb10c4d08c3d98df565664ac5582e" +
                            "494314d5c3c92dddedd5d316a32a206be4ec0846" +
                            "16fe57be15e27cad111aa3c21fa79e32258c6ca8" +
                            "430afc69eddd52d3b751b37da6b6860910b94653" +
                            "192c0db1d02abcfd6ce14c01f238eec7c20bd3bb" +
                            "750940004bacba2880349a9494d10e139ecb2355" +
                            "d101024100ffdc3defd9c05a2d377ef6019fa62b" +
                            "3fbd5b0020a04cc8533bca730e1f6fcf5dfceea1" +
                            "b044fbe17d9eababfbc7d955edad6bc60f9be826" +
                            "ad2c22ba77d19a9f65024100dc28d43fdbbc9385" +
                            "2cc3567093157702bc16f156f709fb7db0d9eec0" +
                            "28f41fd0edcd17224c866e66be1744141fb724a1" +
                            "0fd741c8a96afdd9141b36d67fff6309024077b1" +
                            "cddbde0f69604bdcfe33263fb36ddf24aa3b9922" +
                            "327915b890f8a36648295d0139ecdf68c245652c" +
                            "4489c6257b58744fbdd961834a4cab201801a3b1" +
                            "e52d024100b17142e8991d1b350a0802624759d4" +
                            "8ae2b8071a158ff91fabeb6a8f7c328e762143dc" +
                            "726b8529f42b1fab6220d1c676fdc27ba5d44e84" +
                            "7c72c52064afd351a902407c6e23fe35bcfcd1a6" +
                            "62aa82a2aa725fcece311644d5b6e3894853fd4c" +
                            "e9fe78218c957b1ff03fc9e5ef8ffeb6bd58235f" +
                            "6a215c97d354fdace7e781e4a63e8b"));
            PrivateKey privateKey = keyFactory.generatePrivate(keySpec);
            Certificate[] certs = { CertificateFactory
                    .getInstance("X.509")
                    .generateCertificate(
                            new ByteArrayInputStream(
                                    StringUtils.convertHexToBytes(
                            "3082018b3081f502044295ce6b300d06092a8648" +
                            "86f70d0101040500300d310b3009060355040313" +
                            "024832301e170d3035303532363133323630335a" +
                            "170d3337303933303036353734375a300d310b30" +
                            "0906035504031302483230819f300d06092a8648" +
                            "86f70d010101050003818d0030818902818100dc" +
                            "0a13c602b7141110eade2f051b54777b060d0f74" +
                            "e6a110f9cce81159f271ebc88d8e8aa1f743b505" +
                            "fc2e7dfe38d33b8d3f64d1b363d1af4d87783389" +
                            "7954cbaec2fa384c22a415498cf306bb07ac09b7" +
                            "6b001cd68bf77ea0a628f5101959cf2993a9c23d" +
                            "bee79b19305977f8715ae78d023471194cc900b2" +
                            "31eecb0aaea98d0203010001300d06092a864886" +
                            "f70d01010405000381810083f4401a279453701b" +
                            "ef9a7681a5b8b24f153f7d18c7c892133d97bd5f" +
                            "13736be7505290a445a7d5ceb75522403e509751" +
                            "5cd966ded6351ff60d5193de34cd36e5cb04d380" +
                            "398e66286f99923fd92296645fd4ada45844d194" +
                            "dfd815e6cd57f385c117be982809028bba1116c8" +
                            "5740b3d27a55b1a0948bf291ddba44bed337b9"))), };
            store.setKeyEntry("h2", privateKey, password.toCharArray(), certs);
            // --- generated code end ---
            return store;
        } catch (Exception e) {
            throw DbException.convertToIOException(e);
        }
    }

    private static void setKeystore() throws IOException {
        Properties p = System.getProperties();
        if (p.getProperty(KEYSTORE_KEY) == null) {
            String fileName = KEYSTORE;
            byte[] data = getKeyStoreBytes(getKeyStore(
                    KEYSTORE_PASSWORD), KEYSTORE_PASSWORD);
            boolean needWrite = true;
            if (FileUtils.exists(fileName) && FileUtils.size(fileName) == data.length) {
                // don't need to overwrite the file if it did not change
                InputStream fin = FileUtils.newInputStream(fileName);
                byte[] now = IOUtils.readBytesAndClose(fin, 0);
                if (now != null && Arrays.equals(data, now)) {
                    needWrite = false;
                }
            }
            if (needWrite) {
                try {
                    OutputStream out = FileUtils.newOutputStream(fileName, false);
                    out.write(data);
                    out.close();
                } catch (Exception e) {
                    throw DbException.convertToIOException(e);
                }
            }
            String absolutePath = FileUtils.toRealPath(fileName);
            System.setProperty(KEYSTORE_KEY, absolutePath);
        }
        if (p.getProperty(KEYSTORE_PASSWORD_KEY) == null) {
            System.setProperty(KEYSTORE_PASSWORD_KEY, KEYSTORE_PASSWORD);
        }
    }

    private static String[] enableAnonymous(String[] enabled, String[] supported) {
        LinkedHashSet<String> set = new LinkedHashSet<>();
        for (String x : supported) {
            if (!x.startsWith("SSL") && x.contains("_anon_") &&
                    (x.contains("_AES_") || x.contains("_3DES_")) && x.contains("_SHA")) {
                set.add(x);
            }
        }
        Collections.addAll(set, enabled);
        return set.toArray(new String[0]);
    }

    private static String[] disableSSL(String[] enabled) {
        HashSet<String> set = new HashSet<>();
        for (String x : enabled) {
            if (!x.startsWith("SSL")) {
                set.add(x);
            }
        }
        return set.toArray(new String[0]);
    }

}
