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

package org.apache.ignite.internal.encryption;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.distributed.FullMessage;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.junit.Test;

import static org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi.DEFAULT_MASTER_KEY_NAME;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/** Tests master key change process with master key consistency issues. */
@SuppressWarnings("ThrowableNotThrown")
public class MasterKeyChangeConsistencyCheckTest extends AbstractEncryptionTest {
    /** */
    private final AtomicBoolean simulateOtherDigest = new AtomicBoolean();

    /** */
    private final AtomicBoolean simulateSetMasterKeyError = new AtomicBoolean();

    /** The first handlered failure. */
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        TestKeystoreEncryptionSpi encSpi = new TestKeystoreEncryptionSpi();

        encSpi.setKeyStorePath(keystorePath());
        encSpi.setKeyStorePassword(keystorePassword());

        cfg.setEncryptionSpi(encSpi);

        cfg.setFailureHandler(new TestFailureHandler());

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testRejectMasterKeyChangeWithKeyConsistencyProblems() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        simulateOtherDigest.set(true);

        assertTrue(checkMasterKeyName(DEFAULT_MASTER_KEY_NAME));

        assertThrowsAnyCause(log, () -> grids.get1().encryption().changeMasterKey(MASTER_KEY_NAME_2).get(),
            IgniteException.class, "Master key digest consistency check failed");

        assertTrue(checkMasterKeyName(DEFAULT_MASTER_KEY_NAME));

        simulateOtherDigest.set(false);

        simulateSetMasterKeyError.set(true);

        assertThrowsAnyCause(log, () -> grids.get1().encryption().changeMasterKey(MASTER_KEY_NAME_2).get(),
            IgniteSpiException.class, "Test error.");

        assertTrue(checkMasterKeyName(DEFAULT_MASTER_KEY_NAME));

        simulateSetMasterKeyError.set(false);

        grids.get2().encryption().changeMasterKey(MASTER_KEY_NAME_2).get();

        assertTrue(checkMasterKeyName(MASTER_KEY_NAME_2));
    }

    /** @throws Exception If failed. */
    @Test
    public void testFailureHandledOnFailPerformMasterKeyChange() throws Exception {
        T2<IgniteEx, IgniteEx> grids = startTestGrids(true);

        assertTrue(checkMasterKeyName(DEFAULT_MASTER_KEY_NAME));

        grids.get2().context().discovery().setCustomEventListener(FullMessage.class,
            (topVer, snd, msg) -> simulateSetMasterKeyError.set(true));

        // Expected successful completing the future in case of successful completition of prepare phase and fail
        // of the perform phase.
        grids.get1().encryption().changeMasterKey(MASTER_KEY_NAME_2).get();

        assertNotNull(failure.get());

        assertTrue(X.hasCause(failure.get(), "Test error.", IgniteSpiException.class));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        simulateOtherDigest.set(false);
        simulateSetMasterKeyError.set(false);
    }

    /** */
    private class TestKeystoreEncryptionSpi extends KeystoreEncryptionSpi {
        /** {@inheritDoc} */
        @Override public byte[] masterKeyDigest() {
            if (simulateOtherDigest.get() && ignite.name().equals(GRID_1)) {
                byte[] digest = super.masterKeyDigest();

                digest[0] += 1;

                return digest;
            }

            return super.masterKeyDigest();
        }

        /** {@inheritDoc} */
        @Override public void setMasterKeyName(String name) {
            if (simulateSetMasterKeyError.get() && ignite.name().equals(GRID_1))
                throw new IgniteSpiException("Test error.");

            super.setMasterKeyName(name);
        }
    }

    /** */
    private class TestFailureHandler extends AbstractFailureHandler {
        /** {@inheritDoc} */
        @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
            failure.compareAndSet(null, failureCtx.error());

            return true;
        }
    }
}
