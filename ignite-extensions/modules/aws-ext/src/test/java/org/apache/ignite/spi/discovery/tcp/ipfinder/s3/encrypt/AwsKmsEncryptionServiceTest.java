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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.encryptionsdk.AwsCrypto;
import com.amazonaws.encryptionsdk.CryptoResult;
import com.amazonaws.encryptionsdk.kms.KmsMasterKeyProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Class to test {@link AwsKmsEncryptionService}.
 */
public class AwsKmsEncryptionServiceTest extends GridCommonAbstractTest {
    /**
     * Test encryption and decryption.
     */
    @Test
    public void testEncryptDecrypt() {
        String encKey = "12345";
        byte[] testData = "test string".getBytes(StandardCharsets.UTF_8);
        byte[] encTestData = "enc test string".getBytes(StandardCharsets.UTF_8);

        AwsKmsEncryptionService awsKmsEncryptionSvc = Mockito.spy(new AwsKmsEncryptionService());
        awsKmsEncryptionSvc.setKeyId(encKey)
            .setCredentials(new BasicAWSCredentials("dummy", "dummy"))
            .setRegion(Region.getRegion(Regions.AP_SOUTH_1));

        AwsCrypto awsCrypto = Mockito.mock(AwsCrypto.class);
        KmsMasterKeyProvider prov = Mockito.mock(KmsMasterKeyProvider.class);
        CryptoResult encCryptoRes = Mockito.mock(CryptoResult.class);
        CryptoResult decCryptoRes = Mockito.mock(CryptoResult.class);

        Mockito.doReturn(awsCrypto).when(awsKmsEncryptionSvc).createClient();
        Mockito.doReturn(prov).when(awsKmsEncryptionSvc).createKmsMasterKeyProvider();

        awsKmsEncryptionSvc.init();

        Mockito.doReturn(encCryptoRes).when(awsCrypto).encryptData(prov, testData);
        Mockito.doReturn(encTestData).when(encCryptoRes).getResult();

        Mockito.doReturn(decCryptoRes).when(awsCrypto).decryptData(prov, encTestData);
        Mockito.doReturn(Arrays.asList(encKey)).when(decCryptoRes).getMasterKeyIds();
        Mockito.doReturn(testData).when(decCryptoRes).getResult();

        byte[] encData = awsKmsEncryptionSvc.encrypt(testData);
        byte[] actualOutput = awsKmsEncryptionSvc.decrypt(encData);

        Assert.assertArrayEquals(testData, actualOutput);
    }
}
