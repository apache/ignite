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
import javax.security.auth.x500.X500Principal;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/** Test certificates with explicit validity dates. */
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

    /** */
    public TestCertificateAuthority(String name, long notBefore, long notAfter) throws Exception {
        this.name = new X500Principal("CN=" + name);
        cert = certificate(this.name, keys, notBefore, notAfter, true);
    }

    /** */
    public KeyStore keyStore(String alias, long notBefore, long notAfter) throws Exception {
        KeyPair leafKeys = keyPair();
        X509Certificate leaf = certificate(new X500Principal("CN=" + alias), leafKeys, notBefore, notAfter, false);
        KeyStore store = emptyStore();

        store.setKeyEntry(alias, leafKeys.getPrivate(), PASSWORD, new X509Certificate[] {leaf, cert});

        return store;
    }

    /** */
    public KeyStore trustStore() throws Exception {
        KeyStore store = emptyStore();

        store.setCertificateEntry(name.getName(), cert);

        return store;
    }

    /** */
    static KeyStore merge(KeyStore... stores) throws Exception {
        KeyStore merged = emptyStore();

        for (KeyStore store : stores) {
            for (String alias : Collections.list(store.aliases()))
                merged.setKeyEntry(alias, store.getKey(alias, PASSWORD), PASSWORD, store.getCertificateChain(alias));
        }

        return merged;
    }

    /** */
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
