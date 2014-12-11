/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.hibernate;

import org.apache.commons.dbcp.managed.*;
import org.apache.ignite.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.jta.*;
import org.h2.jdbcx.*;
import org.hibernate.cache.spi.access.*;
import org.hibernate.engine.transaction.internal.jta.*;
import org.hibernate.engine.transaction.spi.TransactionFactory;
import org.hibernate.service.*;
import org.hibernate.service.jdbc.connections.internal.*;
import org.hibernate.service.jdbc.connections.spi.*;
import org.hibernate.service.jta.platform.internal.*;
import org.hibernate.service.jta.platform.spi.*;
import org.jetbrains.annotations.*;
import org.objectweb.jotm.*;

import javax.transaction.*;
import java.util.*;

/**
 *
 * Tests Hibernate L2 cache with TRANSACTIONAL access mode (Hibernate and GridCache are configured
 * to used the same TransactionManager).
 */
public class GridHibernateL2CacheTransactionalSelfTest extends GridHibernateL2CacheSelfTest {
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
    public static class TestTmLookup implements GridCacheTmLookup {
        /** {@inheritDoc} */
        @Override public TransactionManager getTm() throws IgniteCheckedException {
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
    @Override protected GridCacheConfiguration transactionalRegionConfiguration(String regionName) {
        GridCacheConfiguration cfg = super.transactionalRegionConfiguration(regionName);

        cfg.setTransactionManagerLookupClassName(TestTmLookup.class.getName());

        cfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);

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
