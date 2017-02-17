
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Created by amashenkov on 17.02.17.
 */
public class FullTextIndexTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * @return Ignite instance.
     */
    protected Ignite ignite() {
        return grid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        ignite().cache(null).removeAll();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setDiscoverySpi(new TcpDiscoverySpi().setForceServerMode(true).setIpFinder(ipFinder));

        // Otherwise noop swap space will be chosen on Windows.
//        c.setSwapSpaceSpi(new FileSwapSpaceSpi());

        CacheConfiguration cc = defaultCacheConfiguration();

        List<QueryEntity> entityList = new ArrayList<>();

        QueryEntity qryEntity = new QueryEntity();

        qryEntity.setKeyType(Integer.class.getName());
        qryEntity.setValueType(ObjectValue.class.getName());
        qryEntity.addQueryField("strVal", String.class.getName(), null);

        QueryIndex index = new QueryIndex(); // Default index type
        index.setFieldNames(Collections.singletonList("strVal"), true);

        qryEntity.setIndexes(Arrays.asList(index));

        entityList.add(qryEntity);

        qryEntity = new QueryEntity();

        qryEntity.setKeyType(Integer.class.getName());
        qryEntity.setValueType(ObjectValue2.class.getName());
        qryEntity.addQueryField("strVal", String.class.getName(), null);

        index = new QueryIndex();
        index.setIndexType(QueryIndexType.FULLTEXT);
        index.setFieldNames(Collections.singletonList("strVal"), true);

        qryEntity.setIndexes(Arrays.asList(index));

        entityList.add(qryEntity);

        qryEntity = new QueryEntity();

        qryEntity.setKeyType(Integer.class.getName());
        qryEntity.setValueType(String.class.getName());

//        index = new QueryIndex();
//        index.setIndexType(QueryIndexType.SORTED);

        qryEntity.setIndexes(Arrays.asList(index));

        entityList.add(qryEntity);

        cc.setQueryEntities(entityList);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testObjectSortedIndex() throws Exception {
        IgniteCache<Integer, ObjectValue> cache = ignite().cache(null);

        cache.put(1, new ObjectValue("value 1"));
        cache.put(2, new ObjectValue("value 2"));
        cache.put(3, new ObjectValue("value 3"));

        QueryCursor<Cache.Entry<Integer, ObjectValue>> qry
            = cache.query(new SqlQuery<Integer, ObjectValue>(ObjectValue.class, "strVal like ?").setArgs("value%"));

        int expCnt = 3;

        List<Cache.Entry<Integer, ObjectValue>> results = qry.getAll();

        assertEquals(expCnt, results.size());

        qry = cache.query(new SqlQuery<Integer, ObjectValue>(ObjectValue.class, "strVal > ?").setArgs("value 1"));

        results = qry.getAll();

        assertEquals(expCnt - 1, results.size());

        qry = cache.query(new TextQuery<Integer, ObjectValue>(ObjectValue.class, "value"));

        results = qry.getAll();

        assertEquals(0, results.size());
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testObjectTextIndex() throws Exception {
        IgniteCache<Integer, ObjectValue2> cache = ignite(0).cache(null);

        cache.put(1, new ObjectValue2("value 1"));
        cache.put(2, new ObjectValue2("value 2"));
        cache.put(3, new ObjectValue2("value 3"));

        QueryCursor<Cache.Entry<Integer, ObjectValue2>> qry
            = cache.query(new SqlQuery<Integer, ObjectValue2>(ObjectValue2.class, "strVal like ?").setArgs("value%"));

        int expCnt = 3;

        List<Cache.Entry<Integer, ObjectValue2>> results = qry.getAll();

        assertEquals(expCnt, results.size());

        qry = cache.query(new SqlQuery<Integer, ObjectValue2>(ObjectValue2.class, "strVal > ?").setArgs("value 1"));

        results = qry.getAll();

        assertEquals(expCnt - 1, results.size());

        qry = cache.query(new TextQuery<Integer, ObjectValue2>(ObjectValue2.class, "value"));

        results = qry.getAll();

        assertEquals(3, results.size());
    }

    /**
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    public void testStringDefaultIndex() throws Exception {
        IgniteCache<Integer, String> cache = ignite(0).cache(null);

        cache.put(1, "value 1");
        cache.put(2, "value 2");
        cache.put(3, "value 3");

        QueryCursor<Cache.Entry<Integer, String>> qry
            = cache.query(new SqlQuery<Integer, String>(String.class, "_val like ?").setArgs("value%"));

        int expCnt = 3;

        List<Cache.Entry<Integer, String>> results = qry.getAll();

        assertEquals(expCnt, results.size());

        qry = cache.query(new SqlQuery<Integer, String>(String.class, "_val > ?").setArgs("value 1"));

        results = qry.getAll();

        assertEquals(expCnt - 1, results.size());

        qry = cache.query(new TextQuery<Integer, String>(String.class, "value"));

        results = qry.getAll();

        // There is no way to disable FULLTEXT index. So, next line will fails.
        assertEquals(0, results.size());
    }

    /**
     * Another test value object.
     */
    private static class ObjectValue {
        /** Value. */
        private String strVal;

        /**
         * @param strVal String value.
         */
        ObjectValue(String strVal) {
            this.strVal = strVal;
        }

        /**
         * Gets value.
         *
         * @return Value.
         */
        public String value() {
            return strVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ObjectValue other = (ObjectValue)o;

            return strVal == null ? other.strVal == null : strVal.equals(other.strVal);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return strVal != null ? strVal.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ObjectValue.class, this);
        }
    }

    /**
     * Another test value object.
     */
    private static class ObjectValue2 {
        /** Value. */
        private String strVal;

        /**
         * @param strVal String value.
         */
        ObjectValue2(String strVal) {
            this.strVal = strVal;
        }

        /**
         * Gets value.
         *
         * @return Value.
         */
        public String value() {
            return strVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ObjectValue2 other = (ObjectValue2)o;

            return strVal == null ? other.strVal == null : strVal.equals(other.strVal);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return strVal != null ? strVal.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ObjectValue2.class, this);
        }
    }
}
