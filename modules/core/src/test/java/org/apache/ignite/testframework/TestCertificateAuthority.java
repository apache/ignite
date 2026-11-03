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

package org.apache.ignite.testframework;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreSpi;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.x500.X500Principal;
import org.apache.ignite.ssl.SslBundle;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * Creates test stores protected by {@link #PWD}. Private keys use EC P-256, and trust stores hold authority certificates.
 * Certificates use SHA256withECDSA signatures. Issued leaf certificates allow both server and client authentication,
 * so the same material works for node-to-node TLS. Tests choose the issuer and validity dates when those are part of the scenario.
 */
public class TestCertificateAuthority {
    /** Password of every store made here. */
    public static final char[] PWD = "123456".toCharArray();

    /** Name used when certificate identity is not part of the test. */
    private static final String DEFAULT_NAME = "test-certificate";

    /** Certificate serial numbers must be positive and unique within this authority. */
    private final AtomicLong serial = new AtomicLong();

    /** */
    private final X500Principal name;

    /** */
    private final KeyPair keys;

    /** */
    private final X509Certificate cert;

    /**
     * Creates an authority valid from yesterday until a year from now.
     *
     * @param cn Common name of the authority.
     */
    public TestCertificateAuthority(String cn) throws Exception {
        this(cn, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1), System.currentTimeMillis() + TimeUnit.DAYS.toMillis(365));
    }

    /**
     * @param cn Common name of the authority.
     * @param notBefore Time the authority's certificate becomes valid; rounded down to a second.
     * @param notAfter Time the authority's certificate expires; rounded down to a second.
     */
    public TestCertificateAuthority(String cn, long notBefore, long notAfter) throws Exception {
        name = new X500Principal("CN=" + cn);
        keys = keyPair();

        cert = sign(name, keys, notBefore, notAfter, true);
    }

    /** @return Store with one private key and a currently valid certificate whose name is irrelevant to the test. */
    public KeyStore keyStoreWithPrivateKey() throws Exception {
        return keyStoreWithPrivateKey(DEFAULT_NAME);
    }

    /**
     * Issues a certificate valid from an hour ago until a day from now, for tests that do not check expiry.
     *
     * @param cn Common name, also used as the key alias.
     * @return Key store with the private key and a chain containing the issued certificate and this authority's certificate.
     */
    public KeyStore keyStoreWithPrivateKey(String cn) throws Exception {
        long now = System.currentTimeMillis();

        return keyStoreWithPrivateKey(cn, now - TimeUnit.HOURS.toMillis(1), now + TimeUnit.DAYS.toMillis(1));
    }

    /**
     * @param cn Common name of the certificate, also used as the key alias.
     * @param notBefore Time it becomes valid; rounded down to a second.
     * @param notAfter Time it expires; rounded down to a second.
     * @return Key store with the private key and a chain containing the issued certificate and this authority's certificate.
     */
    public KeyStore keyStoreWithPrivateKey(String cn, long notBefore, long notAfter) throws Exception {
        KeyPair pair = keyPair();

        X509Certificate leaf = sign(new X500Principal("CN=" + cn), pair, notBefore, notAfter, false);

        KeyStore store = emptyStore();

        store.setKeyEntry(cn, pair.getPrivate(), PWD, new X509Certificate[] {leaf, cert});

        return store;
    }

    /**
     * @param type Store type.
     * @param alias Private-key alias.
     * @return Saved and reloaded store with a private key but no certificate chain, protected by {@link #PWD}.
     */
    public static KeyStore keyStoreWithoutCertificateChain(String type, String alias) throws Exception {
        KeyStore store = KeyStore.getInstance(type);

        // KeyStore rejects a missing chain on insertion; its provider can still store and load such a key.
        KeyStoreSpi spi = (KeyStoreSpi)store.getProvider().getService("KeyStore", type).newInstance(null);

        spi.engineLoad(null, PWD);
        spi.engineSetKeyEntry(alias, keyPair().getPrivate(), PWD, null);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        spi.engineStore(out, PWD);
        store.load(new ByteArrayInputStream(out.toByteArray()), PWD);

        return store;
    }

    /** @return Trust store with this authority alone. */
    public KeyStore trustStore() throws Exception {
        return trustStore(this);
    }

    /**
     * @param authorities Authorities to trust.
     * @return Trust store with them.
     */
    public static KeyStore trustStore(TestCertificateAuthority... authorities) throws Exception {
        KeyStore store = emptyStore();

        for (TestCertificateAuthority a : authorities)
            store.setCertificateEntry(a.name.getName(), a.cert);

        return store;
    }

    /**
     * @param stores Stores issued here, with distinct aliases.
     * @return Store with the private keys and chains of all of them.
     */
    public static KeyStore merged(KeyStore... stores) throws Exception {
        KeyStore res = emptyStore();

        for (KeyStore store : stores) {
            for (String alias : Collections.list(store.aliases())) {
                if (store.entryInstanceOf(alias, KeyStore.PrivateKeyEntry.class))
                    res.setKeyEntry(alias, store.getKey(alias, PWD), PWD, store.getCertificateChain(alias));
            }
        }

        return res;
    }

    /**
     * @param keyStore Store with exactly one private key.
     * @return Its certificate chain in the form certificate metadata uses.
     */
    public static List<X509Certificate> chain(KeyStore keyStore) throws Exception {
        List<X509Certificate> chain = null;

        for (String alias : Collections.list(keyStore.aliases())) {
            if (!keyStore.entryInstanceOf(alias, KeyStore.PrivateKeyEntry.class))
                continue;

            if (chain != null)
                throw new IllegalArgumentException("The test key store must contain exactly one private key");

            Certificate[] certs = keyStore.getCertificateChain(alias);

            chain = List.of(Arrays.copyOf(certs, certs.length, X509Certificate[].class));
        }

        if (chain == null)
            throw new IllegalArgumentException("The test key store must contain exactly one private key");

        return chain;
    }

    /**
     * @param keyStore Store with the certificate to present.
     * @param trustStore Store with the authorities to trust.
     * @return Context using the supplied key and trust stores.
     */
    public static SSLContext context(KeyStore keyStore, KeyStore trustStore) throws Exception {
        SSLContext ctx = SSLContext.getInstance("TLS");

        ctx.init(keyManagers(keyStore), trustManagers(trustStore), null);

        return ctx;
    }

    /**
     * @param keyStore Generated store with zero or more private keys.
     * @param trustStore Store with the authorities to trust.
     * @return Context and certificate chains from the supplied stores.
     */
    public static SslBundle bundle(KeyStore keyStore, KeyStore trustStore) throws Exception {
        Map<String, List<X509Certificate>> chains = new HashMap<>();

        for (String alias : Collections.list(keyStore.aliases())) {
            if (keyStore.entryInstanceOf(alias, KeyStore.PrivateKeyEntry.class)) {
                Certificate[] certs = keyStore.getCertificateChain(alias);

                chains.put(alias, List.of(Arrays.copyOf(certs, certs.length, X509Certificate[].class)));
            }
        }

        return new SslBundle(context(keyStore, trustStore), chains);
    }

    /**
     * @param trustStore Store with the authorities to trust.
     * @return Trust managers that trust them.
     */
    public static TrustManager[] trustManagers(KeyStore trustStore) throws Exception {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        tmf.init(trustStore);

        return tmf.getTrustManagers();
    }

    /**
     * @param store Store.
     * @param path File to write or replace.
     */
    public static void save(KeyStore store, Path path) throws Exception {
        try (OutputStream out = Files.newOutputStream(path)) {
            store.store(out, PWD);
        }
    }

    /**
     * @param keyStore Store with the certificate to present.
     * @return Key managers that present it.
     */
    private static KeyManager[] keyManagers(KeyStore keyStore) throws Exception {
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

        kmf.init(keyStore, PWD);

        return kmf.getKeyManagers();
    }

    /**
     * @param subj Subject.
     * @param subjKeys Key pair of the subject.
     * @param notBefore Time the certificate becomes valid.
     * @param notAfter Time it expires.
     * @param ca Whether the certificate is the authority's own.
     * @return Certificate signed by this authority.
     */
    private X509Certificate sign(X500Principal subj, KeyPair subjKeys, long notBefore, long notAfter, boolean ca) throws Exception {
        JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
            name,
            BigInteger.valueOf(serial.incrementAndGet()),
            new Date(notBefore),
            new Date(notAfter),
            subj,
            subjKeys.getPublic());

        if (ca)
            builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        else
            builder.addExtension(Extension.extendedKeyUsage, false,
                new ExtendedKeyUsage(new KeyPurposeId[] {KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth}));

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withECDSA").build(keys.getPrivate());

        return new JcaX509CertificateConverter().getCertificate(builder.build(signer));
    }

    /** */
    private static KeyPair keyPair() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("EC");

        gen.initialize(new ECGenParameterSpec("secp256r1"));

        return gen.generateKeyPair();
    }

    /** */
    private static KeyStore emptyStore() throws Exception {
        KeyStore store = KeyStore.getInstance("JKS");

        store.load(null, PWD);

        return store;
    }
}
