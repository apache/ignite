/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.security;

/**
 * A block cipher is a data encryption algorithm that operates on blocks.
 */
public interface BlockCipher {

    /**
     * Blocks sizes are always multiples of this number.
     */
    int ALIGN = 16;

    /**
     * Set the encryption key used for encrypting and decrypting.
     * The key needs to be 16 bytes long.
     *
     * @param key the key
     */
    void setKey(byte[] key);

    /**
     * Encrypt a number of bytes. This is done in-place, that
     * means the bytes are overwritten.
     *
     * @param bytes the byte array
     * @param off the start index
     * @param len the number of bytes to encrypt
     */
    void encrypt(byte[] bytes, int off, int len);

    /**
     * Decrypt a number of bytes. This is done in-place, that
     * means the bytes are overwritten.
     *
     * @param bytes the byte array
     * @param off the start index
     * @param len the number of bytes to decrypt
     */
    void decrypt(byte[] bytes, int off, int len);

    /**
     * Get the length of the key in bytes.
     *
     * @return the length of the key
     */
    int getKeyLength();

}
