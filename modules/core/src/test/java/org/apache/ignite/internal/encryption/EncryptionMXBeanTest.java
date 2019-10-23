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

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.encryption.EncryptionMXBeanImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.EncryptionMXBean;
import org.junit.Test;

import static org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi.DEFAULT_MASTER_KEY_NAME;

/**
 * Tests {@link EncryptionMXBean}
 */
public class EncryptionMXBeanTest extends AbstractEncryptionTest {
    /** @throws Exception If failed. */
    @Test
    public void testMBean() throws Exception {
        IgniteEx ignite = startGrid(GRID_0);

        ignite.cluster().active(true);

        EncryptionMXBean mBean = getMBean();

        assertEquals(DEFAULT_MASTER_KEY_NAME, ignite.encryption().getMasterKeyName());
        assertEquals(DEFAULT_MASTER_KEY_NAME, mBean.getMasterKeyName());

        mBean.changeMasterKey(MASTER_KEY_NAME_2);

        assertEquals(MASTER_KEY_NAME_2, ignite.encryption().getMasterKeyName());
        assertEquals(MASTER_KEY_NAME_2, mBean.getMasterKeyName());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** @return Encryption MBean. */
    private EncryptionMXBean getMBean() throws Exception {
        ObjectName name = U.makeMBeanName(GRID_0, "Encryption", EncryptionMXBeanImpl.class.getSimpleName());

        MBeanServer srv = ManagementFactory.getPlatformMBeanServer();

        assertTrue(srv.isRegistered(name));

        return MBeanServerInvocationHandler.newProxyInstance(srv, name, EncryptionMXBean.class, true);
    }
}
