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

import com.amazonaws.encryptionsdk.MasterKeyProvider;
import com.amazonaws.encryptionsdk.exception.NoSuchMasterKeyException;
import com.amazonaws.encryptionsdk.jce.JceMasterKey;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.ignite.IgniteException;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static com.amazonaws.encryptionsdk.jce.JceMasterKey.getInstance;
import static com.amazonaws.encryptionsdk.multi.MultipleProviderFactory.buildMultiProvider;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.encryption.AbstractEncryptionTest.MASTER_KEY_NAME_2;
import static org.apache.ignite.spi.encryption.AmazonEncryptionSpi.DEFAULT_MASTER_KEY_NAME;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link AmazonEncryptionSpi}.
 */
@SuppressWarnings("ThrowableNotThrown")
public class AmazonEncryptionSpiSelfTest {
    /** */
    @Test
    public void testCantStartWithEmptyParam() {
        assertThrowsWithCause(() -> {
            EncryptionSpi encSpi = new AmazonEncryptionSpi<>();

            encSpi.spiStart("default");
        }, IgniteException.class);
    }

    /** */
    @Test
    public void testCantLoadMasterKeyDoesntExist() {
        assertThrowsWithCause(() -> {
            AmazonEncryptionSpi<JceMasterKey> encSpi = spi();

            encSpi.setMasterKeyName("Unknown key name");

            return null;
        }, NoSuchMasterKeyException.class);
    }

    /** @throws Exception If failed. */
    @Test
    public void testEncryptDecrypt() throws Exception {
        EncryptionSpi encSpi = spi();

        Serializable k = encSpi.create();

        assertNotNull(k);

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

        Serializable k = encSpi.create();

        assertNotNull(k);

        checkKeyEncryptDecrypt(encSpi, k);

        encSpi.setMasterKeyName(MASTER_KEY_NAME_2);

        checkKeyEncryptDecrypt(encSpi, k);
    }

    /** */
    private void checkKeyEncryptDecrypt(EncryptionSpi encSpi, Serializable k) {
        byte[] encGrpKey = encSpi.encryptKey(k);

        assertNotNull(encGrpKey);
        assertTrue(encGrpKey.length > 0);

        Serializable k2 = encSpi.decryptKey(encGrpKey);

        assertEquals(k, k2);
    }

    /** @return Instance of {@link AmazonEncryptionSpi} with {@link JceMasterKey} master key provider. */
    @NotNull private AmazonEncryptionSpi<JceMasterKey> spi() throws Exception {
        AmazonEncryptionSpi<JceMasterKey> encSpi = new AmazonEncryptionSpi<>();

        JceMasterKey key1 = getInstance(retrieveEncryptionKey(),
            "Test provider.", DEFAULT_MASTER_KEY_NAME, "AES/GCM/NoPadding");

        JceMasterKey key2 = getInstance(retrieveEncryptionKey(),
            "Test provider.", MASTER_KEY_NAME_2, "AES/GCM/NoPadding");

        MasterKeyProvider<JceMasterKey> provider = (MasterKeyProvider<JceMasterKey>)buildMultiProvider(key1, key2);

        encSpi.setProvider(provider);

        encSpi.setMasterKeyName(DEFAULT_MASTER_KEY_NAME);

        encSpi.onBeforeStart();

        encSpi.spiStart("default");

        return encSpi;
    }

    /** @return Test secret key. */
    private static SecretKey retrieveEncryptionKey() {
        SecureRandom rnd = new SecureRandom();

        byte[] rawKey = new byte[16]; // 128 bits

        rnd.nextBytes(rawKey);

        return new SecretKeySpec(rawKey, "AES");
    }
}
