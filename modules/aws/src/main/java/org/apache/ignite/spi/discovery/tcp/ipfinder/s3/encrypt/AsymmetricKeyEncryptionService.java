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

package org.apache.ignite.spi.discovery.tcp.ipfinder.s3.encrypt;

import java.security.Key;
import java.security.KeyPair;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Provides an implementation of asymmetric encryption to encrypt/decrypt the data.
 */
public class AsymmetricKeyEncryptionService implements EncryptionService {
    /** Public key. */
    private Key publicKey;

    /** Private key. */
    private Key privateKey;

    /** Encryption service. */
    private Cipher encCipher;

    /** Decryption service. */
    private Cipher decCipher;

    /**
     * Set the public private key pair.
     *
     * @param keyPair Key pair of Public and Private key.
     */
    public void setKeyPair(KeyPair keyPair) {
        if (keyPair.getPublic() == null)
            throw new IgniteException("Public key was not set / was set to null.");

        if (keyPair.getPrivate() == null)
            throw new IgniteException("Private key was not set / was set to null.");

        publicKey = keyPair.getPublic();
        privateKey = keyPair.getPrivate();
    }

    /** {@inheritDoc} */
    @Override public void init() throws IgniteException {
        if (privateKey == null)
            throw new IgniteException("Private key was not set / was set to null.");

        if (publicKey == null)
            throw new IgniteException("Public key was not set / was set to null.");

        encCipher = IgniteUtils.createCipher(privateKey, Cipher.ENCRYPT_MODE);
        decCipher = IgniteUtils.createCipher(publicKey, Cipher.DECRYPT_MODE);
    }

    /** {@inheritDoc} */
    @Override public byte[] encrypt(byte[] data) {
        if (data == null)
            throw new IgniteException("Parameter data cannot be null");

        if (encCipher == null)
            throw new IgniteException("The init() method was not called.");

        try {
            return encCipher.doFinal(data);
        }
        catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] decrypt(byte[] data) {
        if (data == null)
            throw new IgniteException("Parameter data cannot be null");

        if (decCipher == null)
            throw new IgniteException("The init() method was not called.");

        try {
            return decCipher.doFinal(data);
        }
        catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AsymmetricKeyEncryptionService.class, this, "super", super.toString());
    }
}
