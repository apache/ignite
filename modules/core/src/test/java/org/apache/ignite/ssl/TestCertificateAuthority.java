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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.x500.X500Principal;

/**
 * Certificate authority for tests that need certificates valid for a chosen time, which the stores checked into the repository cannot give.
 * Certificates are put together in memory, with the platform's own security providers only.
 */
public class TestCertificateAuthority {
    /** Password of every store made here. */
    public static final char[] PWD = "123456".toCharArray();

    /** DER of {@code ecdsa-with-SHA256}, the algorithm every certificate here is signed with. */
    private static final byte[] SIG_ALG = seq(new byte[] {0x06, 0x08, 0x2A, (byte)0x86, 0x48, (byte)0xCE, 0x3D, 0x04, 0x03, 0x02});

    /** DER of a critical {@code basicConstraints} extension that makes a certificate a CA. */
    private static final byte[] CA_EXT = tlv(0xA3, seq(seq(
        new byte[] {0x06, 0x03, 0x55, 0x1D, 0x13},
        new byte[] {0x01, 0x01, (byte)0xFF},
        tlv(0x04, seq(new byte[] {0x01, 0x01, (byte)0xFF})))));

    /** Format of {@code UTCTime}, which covers the years up to 2049. */
    private static final DateTimeFormatter UTC_TIME = DateTimeFormatter.ofPattern("yyMMddHHmmss'Z'").withZone(ZoneOffset.UTC);

    /** */
    private static final SecureRandom RND = new SecureRandom();

    /** */
    private final X500Principal name;

    /** */
    private final KeyPair keys;

    /** */
    private final X509Certificate cert;

    /** @param cn Common name of the authority. */
    public TestCertificateAuthority(String cn) throws Exception {
        name = new X500Principal("CN=" + cn);
        keys = keyPair();

        long now = System.currentTimeMillis();

        cert = sign(name, keys, now - TimeUnit.DAYS.toMillis(1), now + TimeUnit.DAYS.toMillis(365), true);
    }

    /**
     * @param cn Common name of the certificate.
     * @param notBefore Time it becomes valid; rounded down to a second.
     * @param notAfter Time it expires; rounded down to a second.
     * @return Key store with the certificate, its key and the authority behind it.
     */
    public KeyStore issue(String cn, long notBefore, long notAfter) throws Exception {
        KeyPair pair = keyPair();

        X509Certificate leaf = sign(new X500Principal("CN=" + cn), pair, notBefore, notAfter, false);

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
     * @param keyStore Store with the certificate to present.
     * @param trustStore Store with the authorities to trust.
     * @return Context that presents and trusts them.
     */
    public static SSLContext context(KeyStore keyStore, KeyStore trustStore) throws Exception {
        SSLContext ctx = SSLContext.getInstance("TLS");

        ctx.init(keyManagers(keyStore), trustManagers(trustStore), null);

        return ctx;
    }

    /**
     * @param keyStore Store with the certificate to present.
     * @return Key managers that present it.
     */
    public static KeyManager[] keyManagers(KeyStore keyStore) throws Exception {
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

        kmf.init(keyStore, PWD);

        return kmf.getKeyManagers();
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
     * @param subj Subject.
     * @param subjKeys Key pair of the subject.
     * @param notBefore Time the certificate becomes valid.
     * @param notAfter Time it expires.
     * @param ca Whether the certificate is the authority's own.
     * @return Certificate signed by this authority.
     */
    private X509Certificate sign(X500Principal subj, KeyPair subjKeys, long notBefore, long notAfter, boolean ca) throws Exception {
        byte[] tbs = seq(
            tlv(0xA0, tlv(0x02, new byte[] {2})),
            tlv(0x02, new BigInteger(64, RND).add(BigInteger.ONE).toByteArray()),
            SIG_ALG,
            name.getEncoded(),
            seq(time(notBefore), time(notAfter)),
            subj.getEncoded(),
            subjKeys.getPublic().getEncoded(),
            ca ? CA_EXT : new byte[0]);

        Signature sig = Signature.getInstance("SHA256withECDSA");

        sig.initSign(keys.getPrivate());
        sig.update(tbs);

        byte[] der = seq(tbs, SIG_ALG, bitString(sig.sign()));

        return (X509Certificate)CertificateFactory.getInstance("X.509").generateCertificate(new ByteArrayInputStream(der));
    }

    /** */
    private static KeyPair keyPair() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("EC");

        gen.initialize(new ECGenParameterSpec("secp256r1"));

        return gen.generateKeyPair();
    }

    /** */
    private static KeyStore emptyStore() throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");

        store.load(null, PWD);

        return store;
    }

    /**
     * @param millis Time.
     * @return DER of the time as {@code UTCTime}.
     */
    private static byte[] time(long millis) {
        return tlv(0x17, UTC_TIME.format(Instant.ofEpochMilli(millis)).getBytes());
    }

    /**
     * @param parts DER of the elements.
     * @return DER of the sequence of them.
     */
    private static byte[] seq(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        for (byte[] part : parts)
            out.write(part, 0, part.length);

        return tlv(0x30, out.toByteArray());
    }

    /**
     * @param bytes Bytes, all of their bits used.
     * @return DER of the BIT STRING of them.
     */
    private static byte[] bitString(byte[] bytes) {
        byte[] val = new byte[bytes.length + 1];

        System.arraycopy(bytes, 0, val, 1, bytes.length);

        return tlv(0x03, val);
    }

    /**
     * @param tag Tag.
     * @param val Contents.
     * @return DER of the element.
     */
    private static byte[] tlv(int tag, byte[] val) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        out.write(tag);

        if (val.length < 0x80)
            out.write(val.length);
        else if (val.length < 0x100) {
            out.write(0x81);
            out.write(val.length);
        }
        else {
            out.write(0x82);
            out.write(val.length >> 8);
            out.write(val.length);
        }

        out.write(val, 0, val.length);

        return out.toByteArray();
    }
}
