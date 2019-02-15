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

package org.apache.ignite.cache.hibernate;

import java.util.Collections;
import javax.cache.configuration.Factory;
import javax.transaction.Synchronization;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;
import org.apache.commons.dbcp.managed.BasicManagedDataSource;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.h2.jdbcx.JdbcDataSource;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.jdbc.connections.internal.DatasourceConnectionProviderImpl;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.transaction.jta.platform.internal.AbstractJtaPlatform;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.hibernate.resource.transaction.backend.jta.internal.JtaTransactionCoordinatorBuilderImpl;
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
    public static class TestTmFactory implements Factory<TransactionManager> {
        /** */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override public TransactionManager create() {
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
        if (jotm != null)
            jotm.stop();

        jotm = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxManagerFactory(new TestTmFactory());
        cfg.getTransactionConfiguration().setUseJtaSynchronization(useJtaSynchronization());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration transactionalRegionConfiguration(String regionName) {
        CacheConfiguration cfg = super.transactionalRegionConfiguration(regionName);

        cfg.setNearConfiguration(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Nullable @Override protected StandardServiceRegistryBuilder registryBuilder() {
        StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder();

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

        builder.applySetting(Environment.TRANSACTION_COORDINATOR_STRATEGY, JtaTransactionCoordinatorBuilderImpl.class.getName());

        return builder;
    }

    /** {@inheritDoc} */
    @Override protected AccessType[] accessTypes() {
        return new AccessType[]{AccessType.TRANSACTIONAL};
    }

    /**
     * @return Whether to use {@link Synchronization}.
     */
    protected boolean useJtaSynchronization() {
        return false;
    }
}
