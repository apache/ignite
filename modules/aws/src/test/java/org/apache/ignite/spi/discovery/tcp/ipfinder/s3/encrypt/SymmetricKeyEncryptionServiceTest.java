/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.tcp.ipfinder.s3.encrypt;

import java.nio.charset.StandardCharsets;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Class to test {@link SymmetricKeyEncryptionService}.
 */
public class SymmetricKeyEncryptionServiceTest extends GridCommonAbstractTest {
    /** Symmetric key encryption service. */
    private SymmetricKeyEncryptionService encryptionSvc;

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        byte[] key = "0000000000000000".getBytes(StandardCharsets.UTF_8);
        SecretKey secretKey = new SecretKeySpec(key, "AES");

        encryptionSvc = new SymmetricKeyEncryptionService().setSecretKey(secretKey);
        encryptionSvc.init();
    }

    /**
     * Test whether encryption and decryption.
     */
    @Test
    public void testEncryptDecrypt() {
        byte[] testData = "test string".getBytes(StandardCharsets.UTF_8);
        byte[] encData = encryptionSvc.encrypt(testData);
        byte[] decData = encryptionSvc.decrypt(encData);

        Assert.assertArrayEquals(testData, decData);
    }
}
