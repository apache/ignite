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
