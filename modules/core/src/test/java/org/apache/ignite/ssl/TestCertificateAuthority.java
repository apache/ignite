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

package org.apache.ignite.ssl;

import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.util.Collections;
import java.util.Date;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.security.auth.x500.X500Principal;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * Test CA that issues certificates with explicit validity dates.
 * Each instance has its own EC key and a self-signed certificate.
 * Its key stores hold a leaf certificate signed by the CA, followed by the CA certificate.
 */
public class TestCertificateAuthority {
    /** */
    public static final char[] PASSWORD = "123456".toCharArray();

    /** */
    private final X500Principal name;

    /** */
    private final KeyPair keys = keyPair();

    /** */
    private final X509Certificate cert;

    /** */
    private long serial;

    /**
     * Creates a CA with a self-signed certificate valid between the given dates.
     * The name becomes the common name of the CA certificate.
     */
    public TestCertificateAuthority(String name, long notBefore, long notAfter) throws Exception {
        this.name = new X500Principal("CN=" + name);
        cert = certificate(this.name, keys, notBefore, notAfter, true);
    }

    /** Returns the subject of the CA certificate. */
    public X500Principal subject() {
        return name;
    }

    /**
     * Creates a key store with one private key and its chain under the given alias.
     * The chain holds a leaf certificate with the given dates, signed by this CA, followed by the CA certificate.
     */
    public KeyStore keyStore(String alias, long notBefore, long notAfter) throws Exception {
        return keyStore(alias, notBefore, notAfter, PASSWORD);
    }

    /** Creates the key store with the given password for its private key. */
    public KeyStore keyStore(String alias, long notBefore, long notAfter, char[] pwd) throws Exception {
        KeyPair leafKeys = keyPair();
        X509Certificate leaf = certificate(new X500Principal("CN=" + alias), leafKeys, notBefore, notAfter, false);
        KeyStore store = emptyStore();

        store.setKeyEntry(alias, leafKeys.getPrivate(), pwd, new X509Certificate[] {leaf, cert});

        return store;
    }

    /** Creates key managers for a certificate issued by this CA with the given validity dates. */
    public KeyManager[] keyManagers(String alias, long notBefore, long notAfter) throws Exception {
        KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

        factory.init(keyStore(alias, notBefore, notAfter), PASSWORD);

        return factory.getKeyManagers();
    }

    /**
     * Creates a trust store that trusts only this CA.
     * It holds the CA certificate under the CA name.
     */
    public KeyStore trustStore() throws Exception {
        KeyStore store = emptyStore();

        store.setCertificateEntry(name.getName(), cert);

        return store;
    }

    /** Creates a trust store containing each CA certificate, including CAs with the same subject. */
    public static KeyStore trustStoreOf(TestCertificateAuthority... authorities) throws Exception {
        KeyStore store = emptyStore();

        for (int i = 0; i < authorities.length; i++)
            store.setCertificateEntry(Integer.toString(i), authorities[i].cert);

        return store;
    }

    /**
     * Combines the key entries of several key stores into one key store.
     * Aliases must differ, because an entry with the same alias replaces the earlier one.
     */
    static KeyStore merge(KeyStore... stores) throws Exception {
        KeyStore merged = emptyStore();

        for (KeyStore store : stores) {
            for (String alias : Collections.list(store.aliases()))
                merged.setKeyEntry(alias, store.getKey(alias, PASSWORD), PASSWORD, store.getCertificateChain(alias));
        }

        return merged;
    }

    /**
     * Writes the store to the file with the test password.
     * It replaces an existing file, so a test can put new certificates in place of old ones.
     */
    public static void save(KeyStore store, Path path) throws Exception {
        try (OutputStream out = Files.newOutputStream(path)) {
            store.store(out, PASSWORD);
        }
    }

    /** */
    private X509Certificate certificate(X500Principal subject, KeyPair subjectKeys, long notBefore, long notAfter, boolean ca)
        throws Exception {
        JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
            name,
            BigInteger.valueOf(++serial),
            new Date(notBefore),
            new Date(notAfter),
            subject,
            subjectKeys.getPublic());

        if (ca)
            builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));

        return new JcaX509CertificateConverter().getCertificate(
            builder.build(new JcaContentSignerBuilder("SHA256withECDSA").build(keys.getPrivate())));
    }

    /** */
    private static KeyPair keyPair() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");

        generator.initialize(new ECGenParameterSpec("secp256r1"));

        return generator.generateKeyPair();
    }

    /** */
    private static KeyStore emptyStore() throws Exception {
        KeyStore store = KeyStore.getInstance("JKS");

        store.load(null, PASSWORD);

        return store;
    }
}
