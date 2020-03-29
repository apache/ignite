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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.encryption.AbstractEncryptionTest;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;

/**
 *
 */
public class WalRecoveryWithPageCompressionAndTdeTest extends WalRecoveryWithPageCompressionTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        for (CacheConfiguration<?, ?> ccfg : cfg.getCacheConfiguration())
            ccfg.setEncryptionEnabled(true);

        KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

        encSpi.setKeyStorePath(AbstractEncryptionTest.KEYSTORE_PATH);
        encSpi.setKeyStorePassword(AbstractEncryptionTest.KEYSTORE_PASSWORD.toCharArray());

        cfg.setEncryptionSpi(encSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public void testWalRenameDirSimple() throws Exception {
        // Ignore this test when TDE is enabled, since there is internal cache group id change without corresponding
        // encryption keys change.
    }
}
