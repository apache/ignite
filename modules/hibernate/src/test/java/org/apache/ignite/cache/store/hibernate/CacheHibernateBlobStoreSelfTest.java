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

package org.apache.ignite.cache.store.hibernate;

import java.io.File;
import java.net.URL;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.cache.GridAbstractCacheStoreSelfTest;
import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.Transaction;

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

            if (hTx != null && hTx.isActive())
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
    public void testConfigurationByUrl() throws Exception {
        URL url = U.resolveIgniteUrl(
                "modules/hibernate/src/test/java/org/apache/ignite/cache/store/hibernate/hibernate.cfg.xml");

        assert url != null;

        store.setHibernateConfigurationPath(url.toString());

        // Store will be implicitly initialized.
        store.load("key");
    }

    /**
     * @throws Exception If failed.
     */
    public void testConfigurationByFile() throws Exception {
        URL url = U.resolveIgniteUrl(
                "modules/hibernate/src/test/java/org/apache/ignite/cache/store/hibernate/hibernate.cfg.xml");

        assert url != null;

        File file = new File(url.toURI());

        store.setHibernateConfigurationPath(file.getAbsolutePath());

        // Store will be implicitly initialized.
        store.load("key");
    }

    /**
     * @throws Exception If failed.
     */
    public void testConfigurationByResource() throws Exception {
        store.setHibernateConfigurationPath("/org/apache/ignite/cache/store/hibernate/hibernate.cfg.xml");

        // Store will be implicitly initialized.
        store.load("key");
    }
}