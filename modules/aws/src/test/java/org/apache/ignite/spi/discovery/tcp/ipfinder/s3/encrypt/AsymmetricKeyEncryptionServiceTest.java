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

package org.apache.ignite.spi.discovery.tcp.ipfinder.s3.encrypt;

import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Contains tests for {@link AsymmetricKeyEncryptionService}.
 */
public class AsymmetricKeyEncryptionServiceTest extends GridCommonAbstractTest {
    /** Asymmetric key encryption service. */
    private AsymmetricKeyEncryptionService encryptionSvc;

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        try {
            String algo = "RSA";
            // Public and private key pair is generated using 'openssl'
            String publicKeyStr = "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCbuQ7RcOtsHf2oGQ" +
                "b//cMgfN9kS8tsn21BOAnXwkBN0LwpVXdw1SAfN6fhdJqr4Z585IgF" +
                "EDOlimoDZ2pXHZ6NfmAot4xkioXlsX+lsSir3gMtPfJhtTFvvnvzgr" +
                "ZGWVxu0eLBCiuhlUpYNTHlFaiD8C/Qj7eRY+tUagZRskug8QIDAQAB";

            String privateKeyStr = "MIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBAJu5D" +
                "tFw62wd/agZBv/9wyB832RLy2yfbUE4CdfCQE3QvClVd3DVIB83p+" +
                "F0mqvhnnzkiAUQM6WKagNnalcdno1+YCi3jGSKheWxf6WxKKveAy0" +
                "98mG1MW++e/OCtkZZXG7R4sEKK6GVSlg1MeUVqIPwL9CPt5Fj61Rq" +
                "BlGyS6DxAgMBAAECgYEAj+lILnqitvpIb08hzvYfnCiK8s+xIaN8f" +
                "qdhQUo9zyw2mCRqC5aK5w6yUYNHZc1OgLFamwNMF5KBQsAR4Ix492" +
                "1K8ch4fmqtnaD4wlx3euyH1+ZjmagzutlFHKxKOnFuoaWeWJj0RN2" +
                "f2S3dci2Kh1hkde3PylOgOfKXmz0MfAECQQDMjqEr4KdWnAUwBFgP" +
                "+48wQufpfWzTt2rR7lDxfWoeoo0BlIPVEgvrjmr3mwcX2/kyZK1tD" +
                "Hf9BSTI65a9zl4hAkEAwuJ7mmd/emqXCqgIs8qsLaaNnZUfTTyzb4" +
                "iHgFyh/FEyXeuPN/hyg3Hch2/uA+ZFW+Bc46GSSmzWK4RTJGfI0QJ" +
                "BAI3tHBhUe+ZUxCinqu4T7SpgEYZoNrzCkwPrJRAYoyt0Pv9sqveH" +
                "2Otr2f3H+2jrgAAd6FI0B4BvNDGPe/xfleECQHkopP+RaMeKjOyrG" +
                "v3r+q9G5LQbiaJTIpssnlFHRc3ADTgmwpthcpAVsaziAW+bMXO1QQ" +
                "qj4Hc0wtG7KpVvkIECQBm72Wh6od+BFeWq2iN7XiXIAgXRRvfVTuD" +
                "KFM3vYQlszEsTI2YKcCg2Lg1oFoHn/tuRjOajNs6eWz/0BWzfuHY=";

            PublicKey publicKey = KeyFactory.getInstance(algo)
                .generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(publicKeyStr)));

            PrivateKey privateKey = KeyFactory.getInstance(algo)
                .generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(privateKeyStr)));

            KeyPair keyPair = new KeyPair(publicKey, privateKey);

            encryptionSvc = new AsymmetricKeyEncryptionService();
            encryptionSvc.setKeyPair(keyPair);
            encryptionSvc.init();
        }
        catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            Assert.fail();
        }
    }

    /**
     * Test encryption and decryption.
     */
    @Test
    public void testEncryptDecrypt() {
        byte[] testData = "This is some test data.".getBytes(StandardCharsets.UTF_8);

        byte[] encData = encryptionSvc.encrypt(testData);
        byte[] decData = encryptionSvc.decrypt(encData);

        Assert.assertArrayEquals(testData, decData);
    }
}
