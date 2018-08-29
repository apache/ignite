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

package org.apache.ignite.spi.encryption.aes;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.encryption.EncryptionSpi;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.Nullable;

import static javax.crypto.Cipher.DECRYPT_MODE;
import static javax.crypto.Cipher.ENCRYPT_MODE;

/**
 * EncryptionSPI implementation base on JDK provided cipher algorithm implementations.
 *
 * @see EncryptionSpi
 * @see AESEncryptionKey
 */
public class AESEncryptionSpi extends IgniteSpiAdapter implements EncryptionSpi {
    /**
     * Default key store entry name to store Encryption master key.
     */
    public static final String DEFAULT_MASTER_KEY_NAME = "ignite.master.key";

    /**
     * Algorithm supported by implementation.
     */
    public static final String CIPHER_ALGO = "AES";

    /**
     * Default encryption key size;
     */
    public static final int DEFAULT_KEY_SIZE = 256;

    /**
     * Full name of cipher algorithm.
     */
    private static final String AES_WITH_PADDING = "AES/CBC/PKCS5Padding";

    /**
     * Full name of cipher algorithm without padding.
     */
    private static final String AES_WITHOUT_PADDING = "AES/CBC/NoPadding";

    /**
     * Algorithm used for digest calculation.
     */
    private static final String DIGEST_ALGO = "SHA-512";

    /**
     * Path to master key store.
     */
    private String keyStorePath;

    /**
     * Key store password.
     */
    private char[] keyStorePwd;

    /**
     * Key size.
     */
    private int keySize = DEFAULT_KEY_SIZE;

    /**
     * Master key name.
     */
    private String masterKeyName = DEFAULT_MASTER_KEY_NAME;

    /**
     * Master key.
     */
    private AESEncryptionKey masterKey;

    /** Logger. */
    @LoggerResource
    protected IgniteLogger log;

    /** Ignite */
    @IgniteInstanceResource
    protected Ignite ignite;
    
    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        assertParameter(!F.isEmpty(keyStorePath), "KeyStorePath shouldn't be empty");
        assertParameter(keyStorePwd != null && keyStorePwd.length > 0,
            "KeyStorePassword shouldn't be empty");

        try (InputStream keyStoreFile = keyStoreFile()) {
            assertParameter(keyStoreFile != null, keyStorePath + " doesn't exists!");

            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());

            ks.load(keyStoreFile, keyStorePwd);

            if (log != null)
                log.info("Successfully load keyStore [path=" + keyStorePath + "]");

            masterKey = new AESEncryptionKey(ks.getKey(masterKeyName, keyStorePwd), null);
        }
        catch (GeneralSecurityException | IOException e) {
            throw new IgniteSpiException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        ensureStarted();

        //empty.
    }

    /** {@inheritDoc} */
    @Override public byte[] masterKeyDigest() {
        ensureStarted();

        return makeDigest(masterKey.key().getEncoded());
    }

    /** {@inheritDoc} */
    @Override public AESEncryptionKey create() throws IgniteException {
        ensureStarted();

        try {
            KeyGenerator gen = KeyGenerator.getInstance(CIPHER_ALGO);

            gen.init(keySize);

            SecretKey key = gen.generateKey();

            return new AESEncryptionKey(key, makeDigest(key.getEncoded()));
        }
        catch (NoSuchAlgorithmException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] encrypt(byte[] data, Serializable key, int start, int length) {
        return doEncryption(data, AES_WITH_PADDING, key, start, length);
    }

    /** {@inheritDoc} */
    @Override public byte[] encryptNoPadding(byte[] data, Serializable key, int start, int length) {
        return doEncryption(data, AES_WITHOUT_PADDING, key, start, length);
    }

    /** {@inheritDoc} */
    @Override public byte[] decrypt(byte[] data, Serializable key) {
        return doDecryption(data, AES_WITH_PADDING, key);
    }

    /** {@inheritDoc} */
    @Override public byte[] decryptNoPadding(byte[] data, Serializable key) {
        return doDecryption(data, AES_WITHOUT_PADDING, key);
    }

    /**
     * @param data Plain data.
     * @param algo Encryption algorithm.
     * @param key Encryption key.
     * @param start Offset in {@code data} array.
     * @param length Length in {@code data} array.
     * @return Encrypted data.
     */
    private byte[] doEncryption(byte[] data, String algo, Serializable key, int start, int length) {
        assert key instanceof AESEncryptionKey;
        assert start >= 0 && length + start <= data.length;

        ensureStarted();

        try {
            SecretKeySpec keySpec = new SecretKeySpec(((AESEncryptionKey)key).key().getEncoded(), CIPHER_ALGO);

            Cipher cipher = Cipher.getInstance(algo);

            byte[] iv = initVector(cipher);

            byte[] res = new byte[encryptedSize(length, algo)];

            System.arraycopy(iv, 0, res, 0, iv.length);

            cipher.init(ENCRYPT_MODE, keySpec, new IvParameterSpec(iv));

            cipher.doFinal(data, start, length, res, iv.length);

            return res;
        }
        catch (ShortBufferException | InvalidAlgorithmParameterException | NoSuchAlgorithmException | InvalidKeyException |
            NoSuchPaddingException | IllegalBlockSizeException | BadPaddingException e) {
            throw new IgniteSpiException(e);
        }
    }

    /**
     * @param data Encrypted data.
     * @param algo Encryption algorithm.
     * @param key Encryption key.
     * @return Decrypted data.
     */
    private byte[] doDecryption(byte[] data, String algo, Serializable key) {
        assert key instanceof AESEncryptionKey;

        ensureStarted();

        try {
            SecretKeySpec keySpec = new SecretKeySpec(((AESEncryptionKey)key).key().getEncoded(), CIPHER_ALGO);

            Cipher cipher = Cipher.getInstance(algo);

            cipher.init(DECRYPT_MODE, keySpec, new IvParameterSpec(data, 0, cipher.getBlockSize()));

            return cipher.doFinal(data, cipher.getBlockSize(), data.length - cipher.getBlockSize());
        }
        catch (InvalidAlgorithmParameterException | NoSuchAlgorithmException | InvalidKeyException |
            NoSuchPaddingException | IllegalBlockSizeException | BadPaddingException e) {
            throw new IgniteSpiException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] encryptKey(Serializable key) {
        assert key instanceof AESEncryptionKey;

        byte[] serKey = U.toBytes(key);

        return encrypt(serKey, masterKey, 0, serKey.length);
    }

    /** {@inheritDoc} */
    @Override public AESEncryptionKey decryptKey(byte[] data) {
        byte[] serKey = decrypt(data, masterKey);

        AESEncryptionKey key = U.fromBytes(serKey);

        byte[] digest = makeDigest(key.key().getEncoded());

        if (!Arrays.equals(key.digest, digest))
            throw new IgniteException("Key is broken!");

        return key;
    }

    /** {@inheritDoc} */
    @Override public int encryptedSize(int dataSize) {
        return encryptedSize(dataSize, AES_WITH_PADDING);
    }

    /** {@inheritDoc} */
    @Override public int encryptedSizeNoPadding(int dataSize) {
        return encryptedSize(dataSize, AES_WITHOUT_PADDING);
    }

    /**
     *
     * @param dataSize
     * @param algo
     * @return
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

        return (dataSize/16 + cntBlocks)*16;
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
     * {@code keyStorePath} could be absolute path or path to classpath resource.
     *
     * @return File for {@code keyStorePath}.
     */
    private InputStream keyStoreFile() throws IOException {
        File abs = new File(keyStorePath);

        if (abs.exists())
            return new FileInputStream(abs);

        URL clsPthRes = AESEncryptionSpi.class.getClassLoader().getResource(keyStorePath);

        if (clsPthRes != null)
            return clsPthRes.openStream();

        return null;
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

    /**
     * Gets path to jdk keyStore that stores master key.
     *
     * @return Key store path.
     */
    public String getKeyStorePath() {
        return keyStorePath;
    }

    /**
     * Sets path to jdk keyStore that stores master key.
     *
     * @param keyStorePath Path to JDK KeyStore.
     */
    public void setKeyStorePath(String keyStorePath) {
        assert !F.isEmpty(keyStorePath) : "KeyStore path shouldn't be empty";
        assert !started() : "Spi already started";

        this.keyStorePath = keyStorePath;
    }

    /**
     * Gets key store password.
     *
     * @return Key store password.
     */
    public char[] getKeyStorePwd() {
        return keyStorePwd;
    }

    /**
     * Sets password to access KeyStore.
     *
     * @param keyStorePassword Password for Key Store.
     */
    public void setKeyStorePassword(char[] keyStorePassword) {
        assert keyStorePassword != null && keyStorePassword.length > 0;
        assert !started() : "Spi already started";

        this.keyStorePwd = keyStorePassword;
    }

    /**
     * Gets encryption key size.
     *
     * @return Encryption key size.
     */
    public int getKeySize() {
        return keySize;
    }

    /**
     * Sets encryption key size.
     *
     * @param keySize Key size.
     */
    public void setKeySize(int keySize) {
        assert !started() : "Spi already started";

        this.keySize = keySize;
    }

    /**
     * Gets master key name.
     *
     * @return Master key name.
     */
    public String getMasterKeyName() {
        return masterKeyName;
    }

    /**
     * Sets mater key name.
     *
     * @param masterKeyName Master key name.
     */
    public void setMasterKeyName(String masterKeyName) {
        assert !started() : "Spi already started";

        this.masterKeyName = masterKeyName;
    }
}
