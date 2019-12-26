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

import java.io.Serializable;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.spi.IgniteSpi;
import org.apache.ignite.spi.IgniteSpiException;

/**
 * SPI provides encryption features for an Ignite.
 */
public interface EncryptionSpi extends IgniteSpi {
    /**
     * Returns master key digest.
     * Should always return same digest for a same key.
     * Digest used for a configuration consistency check.
     *
     * @return Master key digest.
     */
    byte[] masterKeyDigest();

    /**
     * Creates new key for an encryption/decryption of cache persistent data: pages, WAL records.
     *
     * @return Newly created encryption key.
     * @throws IgniteException If key creation failed.
     */
    Serializable create() throws IgniteException;

    /**
     * Encrypts data.
     * 
     * @param data Data to encrypt.
     * @param key Encryption key.
     * @param res Destination buffer.
     */
    void encrypt(ByteBuffer data, Serializable key, ByteBuffer res);

    /**
     * Encrypts data without padding info.
     *
     * @param data Data to encrypt.
     * @param key Encryption key.
     * @param res Destination buffer.
     */
    void encryptNoPadding(ByteBuffer data, Serializable key, ByteBuffer res);

    /**
     * Decrypts data encrypted with {@link #encrypt(ByteBuffer, Serializable, ByteBuffer)}
     * 
     * @param data Data to decrypt.
     * @param key Encryption key.
     */
     byte[] decrypt(byte[] data, Serializable key);

    /**
     * Decrypts data encrypted with {@link #encryptNoPadding(ByteBuffer, Serializable, ByteBuffer)}
     *
     * @param data Data to decrypt.
     * @param key Encryption key.
     */
    void decryptNoPadding(ByteBuffer data, Serializable key, ByteBuffer res);

    /**
     * Encrypts key.
     * Adds some info to check key integrity on decryption.
     *
     * @param key Key to encrypt.
     * @return Encrypted key.
     */
    byte[] encryptKey(Serializable key);

    /**
     * Decrypts key and checks it integrity.
     * 
     * @param key Key to decrypt.
     * @return Encrypted key.
     */
    Serializable decryptKey(byte[] key);

    /**
     * @param dataSize Size of plain data in bytes.
     * @return Size of encrypted data in bytes for padding encryption mode.
     */
    int encryptedSize(int dataSize);

    /**
     * @param dataSize Size of plain data in bytes.
     * @return Size of encrypted data in bytes for nopadding encryption mode.
     */
    int encryptedSizeNoPadding(int dataSize);

    /**
     * @return Encrypted data block size.
     */
    int blockSize();

    /**
     * Gets the current master key name.
     *
     * @return Master key name.
     * @see #setMasterKeyName(String)
     */
    String getMasterKeyName();

    /**
     * Sets master key Name that will be used for keys encryption in {@link #encryptKey(Serializable)} and {@link
     * #decryptKey(byte[])} methods and in the {@link #masterKeyDigest()} method.
     *
     * @param masterKeyName Master key name.
     * @throws IgniteSpiException In case of error.
     */
    void setMasterKeyName(String masterKeyName) throws IgniteSpiException;
}
