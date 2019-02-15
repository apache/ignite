/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.encryption;

import java.io.Serializable;
import java.nio.ByteBuffer;
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
}
