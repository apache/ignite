package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;

import java.util.List;

/**
 * Base class for testing
 */
public abstract class H2DynamicIndexingComplexTest extends AbstractSchemaSelfTest {
    /** Cache mode to test with. */
    private final CacheMode cacheMode;

    /** Cache atomicity mode to test with. */
    private final CacheAtomicityMode atomicityMode;

    /** Node index to initiate operations from. */
    private final int nodeIdx;



    /**
     * Constructor.
     * @param cacheMode Cache mode.
     * @param atomicityMode Cache atomicity mode.
     * @param nodeIdx Node index.
     */
    protected H2DynamicIndexingComplexTest(CacheMode cacheMode, CacheAtomicityMode atomicityMode, int nodeIdx) {
        this.cacheMode = cacheMode;
        this.atomicityMode = atomicityMode;
        this.nodeIdx = nodeIdx;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    /** Do test. */
    public void testA() {
        executeSql("CREATE TABLE person (id int, name varchar, age int, company varchar, city varchar, " +
            "primary key (id, name)) WITH \"template=" + cacheMode.name() + ",atomicity=" + atomicityMode.name() + '"');
    }

    /**
     * Run
     * @param stmt S
     * @return
     */
    protected List<List<?>> executeSql(IgniteEx node, String stmt) {
        return node.context().query().querySqlFieldsNoCache(new SqlFieldsQuery(stmt), true).getAll();
    }

    /**
     * Run
     * @param stmt S
     * @return
     */
    protected List<List<?>> executeSql(String stmt) {
        return executeSql(node(), stmt);
    }

    /**
     * @return Node to initiate operations from.
     */
    protected IgniteEx node() {
        return grid(nodeIdx);
    }

    protected IgniteConfiguration commonConfiguration() {
        return new IgniteConfiguration();
    }

    protected IgniteConfiguration clientConfiguration() {
        return commonConfiguration().setClientMode(true);
    }
}
