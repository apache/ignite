package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Collection;

/**
 * Test to check dynamic columns related features.
 */
public class H2DynamicColumnsSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    private final static String CREATE_SQL = "CREATE TABLE Person (id int primary key, name varchar)";

    private final static String DROP_SQL = "DROP TABLE Person";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        run(CREATE_SQL);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        run(DROP_SQL);

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (IgniteConfiguration cfg : configurations())
            Ignition.start(cfg);
    }

    private IgniteConfiguration[] configurations() throws Exception {
        return new IgniteConfiguration[] {
            commonConfiguration(0),
            commonConfiguration(1),
            clientConfiguration(2)
        };
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    public void testBasic() {
        run("ALTER TABLE Person ADD COLUMN age int before id");


    }

    private IgniteConfiguration clientConfiguration(int idx) throws Exception {
        return commonConfiguration(idx).setClientMode(true).setCacheConfiguration(
            new CacheConfiguration<>("idx").setIndexedTypes(Integer.class, Integer.class)
        );
    }

    /**
     * Create common node configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(getTestIgniteInstanceName(idx));

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setMarshaller(new BinaryMarshaller());

        MemoryConfiguration memCfg = new MemoryConfiguration()
            .setDefaultMemoryPolicyName("default")
            .setMemoryPolicies(
                    new MemoryPolicyConfiguration()
                            .setName("default")
                            .setMaxSize(32 * 1024 * 1024L)
                            .setInitialSize(32 * 1024 * 1024L)
            );

        cfg.setMemoryConfiguration(memCfg);

        return optimize(cfg);
    }

    private void run(String sql) {
        grid(0).context().query().querySqlFieldsNoCache(new SqlFieldsQuery(sql).setSchema(QueryUtils.DFLT_SCHEMA), true);
    }
}
