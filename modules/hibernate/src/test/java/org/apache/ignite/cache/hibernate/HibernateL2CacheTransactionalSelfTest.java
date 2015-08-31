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

package org.apache.ignite.cache.hibernate;

import java.util.Collections;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;
import org.apache.commons.dbcp.managed.BasicManagedDataSource;
import org.apache.ignite.cache.jta.CacheTmLookup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.h2.jdbcx.JdbcDataSource;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.engine.transaction.internal.jta.JtaTransactionFactory;
import org.hibernate.engine.transaction.spi.TransactionFactory;
import org.hibernate.service.ServiceRegistryBuilder;
import org.hibernate.service.jdbc.connections.internal.DatasourceConnectionProviderImpl;
import org.hibernate.service.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.service.jta.platform.internal.AbstractJtaPlatform;
import org.hibernate.service.jta.platform.spi.JtaPlatform;
import org.jetbrains.annotations.Nullable;
import org.objectweb.jotm.Jotm;

/**
 *
 * Tests Hibernate L2 cache with TRANSACTIONAL access mode (Hibernate and Cache are configured
 * to used the same TransactionManager).
 */
public class HibernateL2CacheTransactionalSelfTest extends HibernateL2CacheSelfTest {
    /** */
    private static Jotm jotm;

    /**
     */
    private static class TestJtaPlatform extends AbstractJtaPlatform {
        /** {@inheritDoc} */
        @Override protected TransactionManager locateTransactionManager() {
            return jotm.getTransactionManager();
        }

        /** {@inheritDoc} */
        @Override protected UserTransaction locateUserTransaction() {
            return jotm.getUserTransaction();
        }
    }

    /**
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestTmLookup implements CacheTmLookup {
        /** {@inheritDoc} */
        @Override public TransactionManager getTm() {
            return jotm.getTransactionManager();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        jotm = new Jotm(true, false);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        if (jotm != null)
            jotm.stop();

        jotm = null;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration transactionalRegionConfiguration(String regionName) {
        CacheConfiguration cfg = super.transactionalRegionConfiguration(regionName);

        cfg.setTransactionManagerLookupClassName(TestTmLookup.class.getName());

        cfg.setNearConfiguration(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Nullable @Override protected ServiceRegistryBuilder registryBuilder() {
        ServiceRegistryBuilder builder = new ServiceRegistryBuilder();

        DatasourceConnectionProviderImpl connProvider = new DatasourceConnectionProviderImpl();

        BasicManagedDataSource dataSrc = new BasicManagedDataSource(); // JTA-aware data source.

        dataSrc.setTransactionManager(jotm.getTransactionManager());

        dataSrc.setDefaultAutoCommit(false);

        JdbcDataSource h2DataSrc = new JdbcDataSource();

        h2DataSrc.setURL(CONNECTION_URL);

        dataSrc.setXaDataSourceInstance(h2DataSrc);

        connProvider.setDataSource(dataSrc);

        connProvider.configure(Collections.emptyMap());

        builder.addService(ConnectionProvider.class, connProvider);

        builder.addService(JtaPlatform.class, new TestJtaPlatform());

        builder.addService(TransactionFactory.class, new JtaTransactionFactory());

        return builder;
    }

    /** {@inheritDoc} */
    @Override protected AccessType[] accessTypes() {
        return new AccessType[]{AccessType.TRANSACTIONAL};
    }
}