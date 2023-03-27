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

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.ignite.IgniteException;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionKey;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.KEYSTORE_PASSWORD;
import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.KEYSTORE_PATH;
import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.MASTER_KEY_NAME_2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** */
public class KeystoreEncryptionSpiSelfTest {
    /** @throws Exception If failed. */
    @Test
    public void testCantStartWithEmptyParam() throws Exception {
        GridTestUtils.assertThrowsWithCause(() -> {
            EncryptionSpi encSpi = new KeystoreEncryptionSpi();

            encSpi.spiStart("default");
        }, IgniteException.class);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCantStartWithoutPassword() throws Exception {
        GridTestUtils.assertThrowsWithCause(() -> {
            KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

            encSpi.setKeyStorePath("/ignite/is/cool/path/doesnt/exists");

            encSpi.spiStart("default");
        }, IgniteException.class);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCantStartKeystoreDoesntExists() throws Exception {
        GridTestUtils.assertThrowsWithCause(() -> {
            KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

            encSpi.setKeyStorePath("/ignite/is/cool/path/doesnt/exists");
            encSpi.setKeyStorePassword(KEYSTORE_PASSWORD.toCharArray());

            encSpi.spiStart("default");
        }, IgniteException.class);
    }

    /** */
    @Test
    public void testCantLoadMasterKeyDoesntExist() {
        GridTestUtils.assertThrowsWithCause(() -> {
            EncryptionSpi encSpi = spi();

            encSpi.setMasterKeyName("Unknown key");

            return null;
        }, IgniteException.class);
    }

    /** @throws Exception If failed. */
    @Test
    public void testEncryptDecrypt() throws Exception {
        EncryptionSpi encSpi = spi();

        KeystoreEncryptionKey k = (KeystoreEncryptionKey)encSpi.create();

        assertNotNull(k);
        assertNotNull(k.key());

        byte[] plainText = "Just a test string to encrypt!".getBytes(UTF_8);
        byte[] cipherText = new byte[spi().encryptedSize(plainText.length)];

        encSpi.encrypt(ByteBuffer.wrap(plainText), k, ByteBuffer.wrap(cipherText));

        assertNotNull(cipherText);
        assertEquals(encSpi.encryptedSize(plainText.length), cipherText.length);

        byte[] decryptedText = encSpi.decrypt(cipherText, k);

        assertNotNull(decryptedText);
        assertEquals(plainText.length, decryptedText.length);

        assertEquals(new String(plainText, UTF_8), new String(decryptedText, UTF_8));
    }

    /** @throws Exception If failed. */
    @Test
    public void testMasterKeysDigest() throws Exception {
        EncryptionSpi encSpi = spi();

        byte[] digest = encSpi.masterKeyDigest();

        encSpi.setMasterKeyName(MASTER_KEY_NAME_2);

        byte[] digest2 = encSpi.masterKeyDigest();

        assertNotNull(digest);
        assertFalse(Arrays.equals(digest, digest2));
    }

    /** @throws Exception If failed. */
    @Test
    public void testKeyEncryptDecrypt() throws Exception {
        EncryptionSpi encSpi = spi();

        KeystoreEncryptionKey k = (KeystoreEncryptionKey)encSpi.create();

        assertNotNull(k);
        assertNotNull(k.key());

        checkKeyEncryptDecrypt(encSpi, k);

        encSpi.setMasterKeyName(MASTER_KEY_NAME_2);

        checkKeyEncryptDecrypt(encSpi, k);
    }

    /** */
    private void checkKeyEncryptDecrypt(EncryptionSpi encSpi, KeystoreEncryptionKey k) {
        byte[] encGrpKey = encSpi.encryptKey(k);

        assertNotNull(encGrpKey);
        assertTrue(encGrpKey.length > 0);

        KeystoreEncryptionKey k2 = (KeystoreEncryptionKey)encSpi.decryptKey(encGrpKey);

        assertEquals(k.key(), k2.key());
    }

    /** */
    @NotNull private EncryptionSpi spi() throws Exception {
        KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

        encSpi.setKeyStorePath(KEYSTORE_PATH);
        encSpi.setKeyStorePassword(KEYSTORE_PASSWORD.toCharArray());

        GridTestUtils.invoke(encSpi, "onBeforeStart");

        encSpi.spiStart("default");

        return encSpi;
    }
}
