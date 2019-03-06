/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.cache.store.hibernate;

import java.io.File;
import java.net.URL;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.cache.GridAbstractCacheStoreSelfTest;
import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.resource.transaction.spi.TransactionStatus;
import org.junit.Test;

/**
 * Cache store test.
 */
public class CacheHibernateBlobStoreSelfTest extends
    GridAbstractCacheStoreSelfTest<CacheHibernateBlobStore<Object, Object>> {
    /**
     * @throws Exception If failed.
     */
    public CacheHibernateBlobStoreSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        Session s = store.session(null);

        if (s == null)
            return;

        try {
            s.createQuery("delete from " + CacheHibernateBlobStoreEntry.class.getSimpleName())
                    .setFlushMode(FlushMode.ALWAYS).executeUpdate();

            Transaction hTx = s.getTransaction();

            if (hTx != null && hTx.getStatus() == TransactionStatus.ACTIVE)
                hTx.commit();
        }
        finally {
            s.close();
        }
    }

    /** {@inheritDoc} */
    @Override protected CacheHibernateBlobStore<Object, Object> store() {
        return new CacheHibernateBlobStore<>();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConfigurationByUrl() throws Exception {
        URL url = U.resolveIgniteUrl(CacheHibernateStoreFactorySelfTest.MODULE_PATH +
            "/src/test/java/org/apache/ignite/cache/store/hibernate/hibernate.cfg.xml");

        assert url != null;

        store.setHibernateConfigurationPath(url.toString());

        // Store will be implicitly initialized.
        store.load("key");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConfigurationByFile() throws Exception {
        URL url = U.resolveIgniteUrl(CacheHibernateStoreFactorySelfTest.MODULE_PATH +
                "/src/test/java/org/apache/ignite/cache/store/hibernate/hibernate.cfg.xml");

        assert url != null;

        File file = new File(url.toURI());

        store.setHibernateConfigurationPath(file.getAbsolutePath());

        // Store will be implicitly initialized.
        store.load("key");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConfigurationByResource() throws Exception {
        store.setHibernateConfigurationPath("/org/apache/ignite/cache/store/hibernate/hibernate.cfg.xml");

        // Store will be implicitly initialized.
        store.load("key");
    }

}
