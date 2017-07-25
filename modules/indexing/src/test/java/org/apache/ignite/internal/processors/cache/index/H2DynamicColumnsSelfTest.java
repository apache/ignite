package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryField;
import org.apache.ignite.internal.processors.query.QuerySchema;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.h2.table.Column;
import org.h2.value.DataType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Test to check dynamic columns related features.
 */
public abstract class H2DynamicColumnsSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** SQL to create test table. */
    private final static String CREATE_SQL = "CREATE TABLE Person (id int primary key, name varchar)";

    /** SQL to drop test table. */
    private final static String DROP_SQL = "DROP TABLE Person";

    /**
     * Index of coordinator node.
     */
    protected final static int SRV_CRD_IDX = 0;

    /**
     * Index of non coordinator server node.
     */
    protected final static int SRV_IDX = 1;

    /**
     * Index of client.
     */
    protected final static int CLI_IDX = 2;

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

    /**
     * @return Node index to run queries on.
     */
    protected abstract int nodeIndex();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (IgniteConfiguration cfg : configurations())
            Ignition.start(cfg);
    }

    /**
     * @return Grid configurations to start.
     * @throws Exception if failed.
     */
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

    /**
     * Test column addition to the end of the columns list.
     */
    public void testSimpleAddColumn() {
        run("ALTER TABLE Person ADD COLUMN age int");

        QueryField c = c("AGE", Integer.class.getName());

        for (Ignite node : Ignition.allGrids())
            checkNodeState((IgniteEx)node, "PERSON", "NAME", c);
    }

    /**
     * Test column addition before specified column.
     */
    public void testAddColumnBefore() {
        run("ALTER TABLE Person ADD COLUMN age int before id");

        QueryField c = c("AGE", Integer.class.getName());

        for (Ignite node : Ignition.allGrids())
            checkNodeState((IgniteEx)node, "PERSON", null, c);
    }

    /**
     * Test column addition after specified column.
     */
    public void testAddColumnAfter() {
        run("ALTER TABLE Person ADD COLUMN age int after id");

        QueryField c = c("AGE", Integer.class.getName());

        for (Ignite node : Ignition.allGrids())
            checkNodeState((IgniteEx)node, "PERSON", "ID", c);
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
    private IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(getTestIgniteInstanceName(idx));

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

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

    /**
     * Execute SQL command and ignore resulting dataset.
     * @param sql Statement.
     */
    private void run(String sql) {
        grid(nodeIndex()).context().query()
            .querySqlFieldsNoCache(new SqlFieldsQuery(sql).setSchema(QueryUtils.DFLT_SCHEMA), true).getAll();
    }

    /**
     * Check that given columns have been added to all related structures on target node exactly where needed
     *    (namely, schema in cache descriptor, type descriptor on started cache, and H2 state on started cache).
     * @param node Target node.
     * @param tblName Table name to check.
     * @param afterColName Column after which new columns must be added, or {@code null} if they should be
     *     in the beginning of columns list.
     * @param cols Columns whose presence must be checked.
     */
    private static void checkNodeState(IgniteEx node, String tblName, String afterColName, QueryField... cols) {
        String cacheName = QueryUtils.createTableCacheName(QueryUtils.DFLT_SCHEMA, tblName);

        // Schema state check - should pass regardless of cache state.
        {
            DynamicCacheDescriptor desc = node.context().cache().cacheDescriptor(cacheName);

            assertNotNull("Cache descriptor not found", desc);

            assertTrue(desc.sql());

            QuerySchema schema = desc.schema();

            assertNotNull(schema);

            QueryEntity entity = null;

            for (QueryEntity e : schema.entities()) {
                if (F.eq(tblName, e.getTableName())) {
                    entity = e;

                    break;
                }
            }

            assertNotNull("Query entity not found", entity);

            Iterator<Map.Entry<String, String>> it = entity.getFields().entrySet().iterator();

            if (!F.isEmpty(afterColName)) {
                while (it.hasNext()) {
                    Map.Entry<String, String> e = it.next();

                    if (F.eq(afterColName, e.getKey()))
                        break;
                }

                if (!it.hasNext())
                    assertTrue("Column not found: " + afterColName, false);
            }

            for (QueryField col : cols) {
                assertTrue(it.hasNext());

                Map.Entry<String, String> e = it.next();

                assertEquals(col.name(), e.getKey());

                assertEquals(col.typeName(), e.getValue());
            }
        }

        // Start cache on this node if we haven't yet.
        node.cache(cacheName);

        // Type descriptor state check.
        {
            Collection<GridQueryTypeDescriptor> descs = node.context().query().types(cacheName);

            GridQueryTypeDescriptor desc = null;

            for (GridQueryTypeDescriptor d : descs) {
                if (F.eq(tblName, d.tableName())) {
                    desc = d;

                    break;
                }
            }

            assertNotNull("Type descriptor not found", desc);

            Iterator<Map.Entry<String, Class<?>>> it = desc.fields().entrySet().iterator();

            if (!F.isEmpty(afterColName)) {
                while (it.hasNext()) {
                    Map.Entry<String, Class<?>> e = it.next();

                    if (F.eq(afterColName, e.getKey()))
                        break;
                }

                if (!it.hasNext())
                    assertTrue("Column not found: " + afterColName, false);
            }

            for (QueryField col : cols) {
                assertTrue(it.hasNext());

                Map.Entry<String, Class<?>> e = it.next();

                assertEquals(col.name(), e.getKey());

                assertEquals(col.typeName(), e.getValue().getName());
            }
        }



        // H2 table state check.
        {
            GridH2Table tbl = ((IgniteH2Indexing)node.context().query().getIndexing()).dataTable(QueryUtils.DFLT_SCHEMA,
                tblName);

            assertNotNull("Table not found", tbl);

            Iterator<Column> it = Arrays.asList(tbl.getColumns()).iterator();

            for (int i = 0; i < 3; i++)
                it.next();

            if (!F.isEmpty(afterColName)) {
                while (it.hasNext()) {
                    Column c = it.next();

                    if (F.eq(afterColName, c.getName()))
                        break;
                }

                if (!it.hasNext())
                    assertTrue("Column not found: " + afterColName, false);
            }

            for (QueryField col : cols) {
                assertTrue(it.hasNext());

                Column c = it.next();

                assertEquals(col.name(), c.getName());

                assertEquals(col.typeName(), DataType.getTypeClassName(c.getType()));
            }
        }
    }

    private static QueryField c(String name, String typeName) {
        return new QueryField(name, typeName);
    }
}
