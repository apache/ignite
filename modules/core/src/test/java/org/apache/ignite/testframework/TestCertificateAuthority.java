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

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.x500.X500Principal;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * Issues JKS stores protected by {@link #PWD}, each with an EC P-256 private key and its certificate chain.
 * Certificates use SHA256withECDSA signatures. Issued leaf certificates allow both server and client authentication,
 * so the same material works for node-to-node TLS.
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
        long now = System.currentTimeMillis();

        name = new X500Principal("CN=" + cn);
        keys = keyPair();

        cert = sign(name, keys, now - TimeUnit.DAYS.toMillis(1), now + TimeUnit.DAYS.toMillis(365), true);
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
        KeyPair pair = keyPair();

        X509Certificate leaf = sign(new X500Principal("CN=" + cn), pair, now - TimeUnit.HOURS.toMillis(1),
            now + TimeUnit.DAYS.toMillis(1), false);

        KeyStore store = emptyStore();

        store.setKeyEntry(cn, pair.getPrivate(), PWD, new X509Certificate[] {leaf, cert});

        return store;
    }

    /** @return Trust store with this authority alone. */
    public KeyStore trustStore() throws Exception {
        KeyStore store = emptyStore();

        store.setCertificateEntry(name.getName(), cert);

        return store;
    }

    /**
     * @param keyStore Store with at most one private key.
     * @return Its certificate chain, or {@code null} if the store has no private key.
     */
    public static X509Certificate[] certificateChain(KeyStore keyStore) throws Exception {
        X509Certificate[] chain = null;

        for (String alias : Collections.list(keyStore.aliases())) {
            if (!keyStore.entryInstanceOf(alias, KeyStore.PrivateKeyEntry.class))
                continue;

            if (chain != null)
                throw new IllegalArgumentException("The test key store must contain at most one private key");

            Certificate[] certs = keyStore.getCertificateChain(alias);

            chain = Arrays.copyOf(certs, certs.length, X509Certificate[].class);
        }

        return chain;
    }

    /**
     * @param keyStore Store with exactly one private key.
     * @return Its certificate chain in the form certificate metadata uses.
     */
    public static List<X509Certificate> chain(KeyStore keyStore) throws Exception {
        return List.of(certificateChain(keyStore));
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
