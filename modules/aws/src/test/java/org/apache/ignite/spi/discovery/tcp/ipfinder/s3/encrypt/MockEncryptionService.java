/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.tcp.ipfinder.s3.encrypt;

import java.nio.charset.StandardCharsets;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * Class to provide a mock implementation of {@link EncryptionService}.
 */
public class MockEncryptionService implements EncryptionService {
    /** Encryption service. */
    private final EncryptionService encryptionSvc;

    /**
     * Constructor
     *
     * @param encryptionSvc Encryption service.
     */
    private MockEncryptionService(EncryptionService encryptionSvc) {
        this.encryptionSvc = encryptionSvc;
    }

    /**
     * @return An instance of this class.
     */
    public static MockEncryptionService instance() {
        SecretKey secretKey = new SecretKeySpec("0000000000000000".getBytes(StandardCharsets.UTF_8), "AES");
        EncryptionService encryptionSvc = new SymmetricKeyEncryptionService().setSecretKey(secretKey);

        encryptionSvc.init();

        return new MockEncryptionService(encryptionSvc);
    }

    /** {@inheritDoc} */
    @Override public void init() {
        // Nothing to do
    }

    /** {@inheritDoc} */
    @Override public byte[] encrypt(byte[] payload) {
        return encryptionSvc.encrypt(payload);
    }

    /** {@inheritDoc} */
    @Override public byte[] decrypt(byte[] payload) {
        return encryptionSvc.decrypt(payload);
    }
}
