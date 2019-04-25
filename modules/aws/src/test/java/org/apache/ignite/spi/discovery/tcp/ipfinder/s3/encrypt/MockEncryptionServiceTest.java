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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Class to test {@link MockEncryptionService}.
 */
public class MockEncryptionServiceTest extends GridCommonAbstractTest {
    /** Mock encryption service. */
    private MockEncryptionService mockEncryptionSvc;

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        mockEncryptionSvc = MockEncryptionService.instance();
    }

    /**
     * Test if the service correctly encrypts and decrypts data.
     */
    @Test
    public void testEncryptDecrypt() {
        byte[] testStr = "test string".getBytes(StandardCharsets.UTF_8);

        byte[] encData = mockEncryptionSvc.encrypt(testStr);
        byte[] decData = mockEncryptionSvc.decrypt(encData);

        Assert.assertArrayEquals(testStr, decData);
    }
}
