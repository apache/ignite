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

package org.apache.ignite.encryption;

import org.apache.ignite.IgniteException;
import org.apache.ignite.spi.IgniteSpi;

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
    EncryptionKey create() throws IgniteException;

    /**
     * Encrypts data.
     * 
     * @param data Data to encrypt.
     * @param key Encryption key.
     * @param start Offset in {@code data} to start encrypt from.
     * @param length Length of {@code data} to encrypt.
     * @return Encrypted data.
     */
    byte[] encrypt(byte[] data, EncryptionKey key, int start, int length);

    /**
     * Decrypts data.
     * 
     * @param data Data to decrypt.
     * @param key Encryption key.
     * @return Decrypted data.
     */
    byte[] decrypt(byte[] data, EncryptionKey key);

    /**
     * Encrypts key.
     * Adds some info to check key integrity on decryption.
     *
     * @param key Key to encrypt.
     * @return Encrypted key.
     */
    byte[] encryptKey(EncryptionKey key);

    /**
     * Decrypts key and checks it integrity.
     * 
     * @param key Key to decrypt.
     * @return Encrypted key.
     */
    EncryptionKey decryptKey(byte[] key);

    /**
     * @param dataSize Size of plain data in bytes.
     * @return Size of encrypted data in bytes.
     */
    int encryptedSize(int dataSize);
}
