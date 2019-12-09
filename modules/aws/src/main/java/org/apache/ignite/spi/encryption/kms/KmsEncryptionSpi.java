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

package org.apache.ignite.spi.encryption.kms;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.DataKey;
import com.amazonaws.encryptionsdk.MasterKey;
import com.amazonaws.encryptionsdk.MasterKeyProvider;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.jetbrains.annotations.Nullable;

import static com.amazonaws.encryptionsdk.CryptoAlgorithm.ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA256;
import static javax.crypto.Cipher.DECRYPT_MODE;
import static javax.crypto.Cipher.ENCRYPT_MODE;

/**
 * {@link EncryptionSpi} implementation based on <a href='https://aws.amazon.com/kms'>AWS Key Management Service</a>.
 *
 * @see EncryptionSpi
 * @see KmsEncryptionKey
 *
 * @param <M> the concrete type of the {@link MasterKey}
 */
public class KmsEncryptionSpi<M extends MasterKey<M>> extends IgniteSpiAdapter implements EncryptionSpi {
    /**
     * Default master key name.
     */
    public static final String DEFAULT_MASTER_KEY_NAME = "ignite.master.key";

    /**
     * Full name of cipher algorithm.
     */
    private static final String AES_WITH_PADDING = "AES/CBC/PKCS5Padding";

    /**
     * Full name of cipher algorithm without padding.
     */
    private static final String AES_WITHOUT_PADDING = "AES/CBC/NoPadding";

    /**
     * Algorithm supported by implementation.
     */
    public static final String CIPHER_ALGO = "AES";

    /**
     * Algorithm used for digest calculation.
     */
    private static final String DIGEST_ALGO = "SHA-512";

    /**
     * Algorithm used for group key encryption.
     */
    private static final CryptoAlgorithm CRYPTO_ALGO = ALG_AES_256_GCM_IV12_TAG16_HKDF_SHA256;

    /**
     * Data block size.
     */
    private static final int BLOCK_SZ = 16;

    /** Encryption context. */
    private final Map<String, String> encCtx = Collections.emptyMap();

    /** Master key provider. */
    private volatile MasterKeyProvider<M> provider;

    /** Master key name. */
    private volatile String masterKeyName = DEFAULT_MASTER_KEY_NAME;

    /** Master key. */
    private volatile M masterKey;

    /** */
    private static final ThreadLocal<Cipher> aesWithPadding = ThreadLocal.withInitial(() -> {
        try {
            return Cipher.getInstance(AES_WITH_PADDING);
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
            throw new IgniteException(e);
        }
    });

    /** */
    private static final ThreadLocal<Cipher> aesWithoutPadding = ThreadLocal.withInitial(() -> {
        try {
            return Cipher.getInstance(AES_WITHOUT_PADDING);
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
            throw new IgniteException(e);
        }
    });

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        assertParameter(provider != null, "Master key provider should be set.");

        masterKey = provider.getMasterKey(masterKeyName);
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public byte[] masterKeyDigest() {
        return U.toBytes(masterKey.getKeyId());
    }

    /** {@inheritDoc} */
    @Override public KmsEncryptionKey<M> create() throws IgniteException {
        DataKey<M> key = masterKey.generateDataKey(CRYPTO_ALGO, encCtx);

        return new KmsEncryptionKey<>(key);
    }

    /** {@inheritDoc} */
    @Override public void encrypt(ByteBuffer data, Serializable key, ByteBuffer res) {
        doEncryption(data, aesWithPadding.get(), key, res);
    }

    /** {@inheritDoc} */
    @Override public void encryptNoPadding(ByteBuffer data, Serializable key, ByteBuffer res) {
        doEncryption(data, aesWithoutPadding.get(), key, res);
    }

    /** {@inheritDoc} */
    @Override public byte[] decrypt(byte[] data, Serializable key) {
        assert key instanceof KmsEncryptionKey;

        ensureStarted();

        try {
            SecretKeySpec keySpec = new SecretKeySpec(((DataKey<M>)key).getKey().getEncoded(), CIPHER_ALGO);

            Cipher cipher = aesWithPadding.get();

            cipher.init(DECRYPT_MODE, keySpec, new IvParameterSpec(data, 0, cipher.getBlockSize()));

            return cipher.doFinal(data, cipher.getBlockSize(), data.length - cipher.getBlockSize());
        }
        catch (InvalidAlgorithmParameterException | InvalidKeyException | IllegalBlockSizeException |
            BadPaddingException e) {
            throw new IgniteSpiException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void decryptNoPadding(ByteBuffer data, Serializable key, ByteBuffer res) {
        assert key instanceof KmsEncryptionKey;

        ensureStarted();

        try {
            SecretKeySpec keySpec = new SecretKeySpec(((DataKey<M>)key).getKey().getEncoded(), CIPHER_ALGO);

            Cipher cipher = aesWithoutPadding.get();

            byte[] iv = new byte[cipher.getBlockSize()];

            data.get(iv);

            cipher.init(DECRYPT_MODE, keySpec, new IvParameterSpec(iv));

            cipher.doFinal(data, res);
        }
        catch (InvalidAlgorithmParameterException | InvalidKeyException | IllegalBlockSizeException |
            ShortBufferException | BadPaddingException e) {
            throw new IgniteSpiException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] encryptKey(Serializable key) {
        assert key instanceof KmsEncryptionKey;

        DataKey<?> encKey = masterKey.encryptDataKey(CRYPTO_ALGO, encCtx, (DataKey<?>)key);

        int encKeyLen = 4 + encKey.getProviderInformation().length + 4 + encKey.getEncryptedDataKey().length;

        ByteBuffer buf = ByteBuffer.allocate(encKeyLen);

        buf.putInt(encKey.getProviderInformation().length);

        buf.put(encKey.getProviderInformation());

        buf.putInt(encKey.getEncryptedDataKey().length);

        buf.put(encKey.getEncryptedDataKey());

        return buf.array();
    }

    /** {@inheritDoc} */
    @Override public KmsEncryptionKey<M> decryptKey(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);

        int providerInfoLen = buf.getInt();

        byte[] providerInfo = new byte[providerInfoLen];

        buf.get(providerInfo);

        int encKeyLen = buf.getInt();

        byte[] encKeyBytes = new byte[encKeyLen];

        buf.get(encKeyBytes);

        DataKey<M> encKey = new DataKey<>(null, encKeyBytes, providerInfo, masterKey);

        DataKey<M> key = masterKey.decryptDataKey(CRYPTO_ALGO, Collections.singleton(encKey), encCtx);

        return new KmsEncryptionKey<>(key);
    }

    /** {@inheritDoc} */
    @Override public int encryptedSize(int dataSize) {
        return encryptedSize(dataSize, AES_WITH_PADDING);
    }

    /** {@inheritDoc} */
    @Override public int encryptedSizeNoPadding(int dataSize) {
        return encryptedSize(dataSize, AES_WITHOUT_PADDING);
    }

    /** {@inheritDoc} */
    @Override public int blockSize() {
        return BLOCK_SZ;
    }

    /** {@inheritDoc} */
    @Override public String getMasterKeyName() {
        return masterKeyName;
    }

    /** {@inheritDoc} */
    @Override public void setMasterKeyName(String masterKeyName) throws IgniteSpiException {
        if (started())
            masterKey = provider.getMasterKey(masterKeyName);

        this.masterKeyName = masterKeyName;
    }

    /**
     * Sets master key provider.
     *
     * @param provider Master key provider.
     */
    public void setProvider(MasterKeyProvider<M> provider) {
        if (started())
            throw new IgniteSpiException("Master key provider can not be changed. SPIs already started.");

        this.provider = provider;
    }

    /**
     * @param data Plain data.
     * @param cipher Cipher.
     * @param key Encryption key.
     */
    private void doEncryption(ByteBuffer data, Cipher cipher, Serializable key, ByteBuffer res) {
        assert key instanceof KmsEncryptionKey;

        ensureStarted();

        try {
            SecretKeySpec keySpec = new SecretKeySpec(((DataKey<M>)key).getKey().getEncoded(), CIPHER_ALGO);

            byte[] iv = initVector(cipher);

            res.put(iv);

            cipher.init(ENCRYPT_MODE, keySpec, new IvParameterSpec(iv));

            cipher.doFinal(data, res);
        }
        catch (ShortBufferException | InvalidAlgorithmParameterException | InvalidKeyException |
            IllegalBlockSizeException | BadPaddingException e) {
            throw new IgniteSpiException(e);
        }
    }

    /**
     * @param cipher Cipher.
     * @return Init vector for encryption.
     * @see <a href="https://en.wikipedia.org/wiki/Initialization_vector">Initialization vector</a>
     */
    private byte[] initVector(Cipher cipher) {
        byte[] iv = new byte[cipher.getBlockSize()];

        ThreadLocalRandom.current().nextBytes(iv);

        return iv;
    }

    /**
     * Calculates message digest.
     *
     * @param msg Message.
     * @return Digest.
     */
    private byte[] makeDigest(byte[] msg) {
        try {
            MessageDigest md = MessageDigest.getInstance(DIGEST_ALGO);

            return md.digest(msg);
        }
        catch (NoSuchAlgorithmException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param dataSize Data size.
     * @param algo Encryption algorithm
     * @return Encrypted data size.
     */
    private int encryptedSize(int dataSize, String algo) {
        int cntBlocks;

        switch (algo) {
            case AES_WITH_PADDING:
                cntBlocks = 2;
                break;

            case AES_WITHOUT_PADDING:
                cntBlocks = 1;
                break;

            default:
                throw new IllegalStateException("Unknown algorithm: " + algo);
        }

        return (dataSize / BLOCK_SZ + cntBlocks) * BLOCK_SZ;
    }

    /**
     * Ensures spi started.
     *
     * @throws IgniteException If spi not started.
     */
    private void ensureStarted() throws IgniteException {
        if (started())
            return;

        throw new IgniteException("EncryptionSpi is not started!");
    }
}
