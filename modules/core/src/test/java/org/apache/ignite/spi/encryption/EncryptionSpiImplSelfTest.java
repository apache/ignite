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

package org.apache.ignite.spi.encryption;

import junit.framework.TestCase;
import org.apache.ignite.IgniteException;
import org.apache.ignite.encryption.EncryptionSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.KEYSTORE_PASSWORD;
import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.KEYSTORE_PATH;

/** */
public class EncryptionSpiImplSelfTest extends TestCase {
    /** @throws Exception If failed. */
    public void testCantStartWithEmptyParam() throws Exception {
        GridTestUtils.assertThrowsWithCause(() -> {
            EncryptionSpi encSpi = new EncryptionSpiImpl();

            encSpi.spiStart("default");
        }, IgniteException.class);
    }

    /** @throws Exception If failed. */
    public void testCantStartWithoutPassword() throws Exception {
        GridTestUtils.assertThrowsWithCause(() -> {
            EncryptionSpiImpl encSpi = new EncryptionSpiImpl();

            encSpi.setKeyStorePath("/ignite/is/cool/path/doesnt/exists");

            encSpi.spiStart("default");
        }, IgniteException.class);
    }

    /** @throws Exception If failed. */
    public void testCantStartKeystoreDoesntExists() throws Exception {
        GridTestUtils.assertThrowsWithCause(() -> {
            EncryptionSpiImpl encSpi = new EncryptionSpiImpl();

            encSpi.setKeyStorePath("/ignite/is/cool/path/doesnt/exists");
            encSpi.setKeyStorePassword(KEYSTORE_PASSWORD.toCharArray());

            encSpi.spiStart("default");
        }, IgniteException.class);
    }

    /** @throws Exception If failed. */
    public void testEncryptDecrypt() throws Exception {
        EncryptionSpi encSpi = spi();

        EncryptionKeyImpl k = GridTestUtils.getFieldValue(encSpi, "masterKey");
        
        assertNotNull(k);
        assertNotNull(k.key());

        byte[] plainText = "Just a test string to encrypt!".getBytes("UTF-8");

        byte[] cipherText = encSpi.encrypt(plainText, k);

        assertNotNull(cipherText);
        assertEquals(encSpi.encryptedSize(plainText.length), cipherText.length);
        
        byte[] decryptedText = encSpi.decrypt(cipherText, k);

        assertNotNull(decryptedText);
        assertEquals(plainText.length, decryptedText.length);

        assertEquals(new String(plainText, "UTF-8"), new String(decryptedText, "UTF-8"));
    }

    /** @throws Exception If failed. */
    public void testKeyEncryptDecrypt() throws Exception {
        EncryptionSpi encSpi = spi();
        
        EncryptionKeyImpl k = (EncryptionKeyImpl)encSpi.create();

        assertNotNull(k);
        assertNotNull(k.key());

        byte[] encGrpKey = encSpi.encryptKey(k);

        assertNotNull(encGrpKey);
        assertTrue(encGrpKey.length > 0);

        EncryptionKeyImpl k2 = (EncryptionKeyImpl)encSpi.decryptKey(encGrpKey);

        assertEquals(k.key(), k2.key());
    }

    /** */
    @NotNull private EncryptionSpi spi() throws Exception {
        EncryptionSpiImpl encSpi = new EncryptionSpiImpl();

        encSpi.setKeyStorePath(KEYSTORE_PATH);
        encSpi.setKeyStorePassword(KEYSTORE_PASSWORD.toCharArray());

        GridTestUtils.invoke(encSpi, "onBeforeStart");

        encSpi.spiStart("default");

        return encSpi;
    }
}
