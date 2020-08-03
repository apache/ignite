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

import java.util.List;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CryptoResult;
import com.amazonaws.encryptionsdk.kms.KmsMasterKey;
import com.amazonaws.encryptionsdk.kms.KmsMasterKeyProvider;
import com.amazonaws.regions.Region;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Provides an implementation of AWS KMS to encrypt/decrypt the data.
 */
public class AwsKmsEncryptionService implements EncryptionService {
    /** KMS Key id. */
    private String keyId;

    /** AWS Region. */
    private Region region;

    /** AWS Credentials to access the key. */
    private AWSCredentials creds;

    /** AWS Client conf. */
    private ClientConfiguration clientConf = new ClientConfiguration();

    /** Kms master key provider. */
    private KmsMasterKeyProvider prov;

    /** Aws crypto. */
    private AwsCrypto crypto;

    /**
     * Set the KMS key id used to encrypt/decrypt the data.
     *
     * @param keyId Key id.
     * @return {@code this} for chaining.
     */
    public AwsKmsEncryptionService setKeyId(String keyId) {
        this.keyId = keyId;

        return this;
    }

    /**
     * AWS region.
     *
     * @param region Region.
     * @return {@code this} for chaining.
     */
    public AwsKmsEncryptionService setRegion(Region region) {
        this.region = region;

        return this;
    }

    /**
     * AWS credentials.
     *
     * @param creds Aws Credentials.
     * @return {@code this} for chaining.
     */
    public AwsKmsEncryptionService setCredentials(AWSCredentials creds) {
        this.creds = creds;

        return this;
    }

    /**
     * AWS client configuration.
     *
     * @param clientConf Client conf.
     * @return {@code this} for chaining.
     */
    public AwsKmsEncryptionService setClientConf(ClientConfiguration clientConf) {
        this.clientConf = clientConf;

        return this;
    }

    /** {@inheritDoc} */
    @Override public void init() {
        if (creds == null || region == null || keyId == null || keyId.trim().isEmpty())
            throw new IgniteException(String.format("At-least one of the required parameters " +
                "[creds = %s, region = %s, keyId = %s] is invalid.", creds, region, keyId));

        crypto = createClient();

        prov = createKmsMasterKeyProvider();
    }

    /** {@inheritDoc} */
    @Override public byte[] encrypt(byte[] data) {
        if (crypto == null || prov == null)
            throw new IgniteException("The init() method was not called.");

        return crypto.encryptData(prov, data).getResult();
    }

    /** {@inheritDoc} */
    @Override public byte[] decrypt(byte[] data) {
        if (crypto == null || prov == null)
            throw new IgniteException("The init() method was not called.");

        CryptoResult<byte[], KmsMasterKey> decryptRes = crypto.decryptData(prov, data);

        List<String> keyIds = decryptRes.getMasterKeyIds();

        if (keyIds != null && !keyIds.contains(keyId))
            throw new IgniteException("Wrong KMS key ID!");

        return decryptRes.getResult();
    }

    /**
     * @return An instance of {@link AwsCrypto}.
     */
    AwsCrypto createClient() {
        return crypto = new AwsCrypto();
    }

    /**
     * @return An instance of {@link KmsMasterKeyProvider}.
     */
    KmsMasterKeyProvider createKmsMasterKeyProvider() {
        return new KmsMasterKeyProvider(new AWSStaticCredentialsProvider(creds), region, clientConf, keyId);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AwsKmsEncryptionService.class, this, "super", super.toString());
    }
}
