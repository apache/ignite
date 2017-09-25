/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.apache.ignite.internal.processors.query;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test sql queries with parameters for all types The test fix IGNITE-6286
 *
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 * @see <a href="https://issues.apache.org/jira/browse/IGNITE-6286">IGNITE-6286</a>
 */
public class IgniteSqlParameterizedQueryTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    private static String CACHE_BOOKMARK = "Bookmark";
    private static String NODE_CLIENT = "client";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        c.setCacheConfiguration(buildCacheConfiguration(CACHE_BOOKMARK));
        if (gridName.equals(NODE_CLIENT))
            c.setClientMode(true);

        return c;
    }

    private CacheConfiguration buildCacheConfiguration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration(name);
        QueryEntity bookmarkQueryEntity = new QueryEntity(String.class.getName(), Bookmark.class.getName());

        for (Field currentField : Bookmark.class.getDeclaredFields()) {
            bookmarkQueryEntity.addQueryField(currentField.getName(), currentField.getType().getName(), null);
        }
        log.info("createEntityCacheConfiguration. full QueryEntity info : :"+bookmarkQueryEntity.toString());
        List<QueryEntity> queryEntities = new ArrayList<>();
        queryEntities.add(bookmarkQueryEntity);
        ccfg.setQueryEntities(queryEntities);
        return ccfg;

    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(0);
        startGrid(NODE_CLIENT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    private Bookmark searchBookmarkBy(String field, Object value) {
        IgniteCache<String, Bookmark> cache = grid(NODE_CLIENT).cache(CACHE_BOOKMARK);
        SqlFieldsQuery query = new SqlFieldsQuery("SELECT _val from  Bookmark where "+field+"=?");
        query.setArgs(value);

        QueryCursor<List<?>> cursor = cache.query(query);
        List<List<?>> results = cursor.getAll();
        assertEquals(1, results.size());
        List<?> row0 = results.get(0);
        return (Bookmark)row0.get(0);
    }

    public void testStringSupport() throws Exception {

        IgniteCache<String, Bookmark> cache = grid(NODE_CLIENT).cache(CACHE_BOOKMARK);
        Bookmark bookmark = new Bookmark();
        bookmark.setId(UUID.randomUUID().toString());
        bookmark.setDescription("description");
        cache.put(bookmark.id, bookmark);

        Bookmark loadedBookmark = searchBookmarkBy("description", "description");
        assertEquals("String value does not match", bookmark.getDescription(), loadedBookmark.getDescription());
        assertEquals("Id value does not match", bookmark.getId(), loadedBookmark.getId());
    }

    public void testIntegerSupport() throws Exception {

        IgniteCache<String, Bookmark> cache = grid(NODE_CLIENT).cache(CACHE_BOOKMARK);
        Bookmark bookmark = new Bookmark();
        bookmark.setId(UUID.randomUUID().toString());
        bookmark.setStockCount(Integer.MAX_VALUE);
        cache.put(bookmark.id, bookmark);

        Bookmark loadedBookmark = searchBookmarkBy("stockCount", Integer.MAX_VALUE);
        assertEquals("Integer value does not match", bookmark.getStockCount(), loadedBookmark.getStockCount());
        assertEquals("Id value does not match", bookmark.getId(), loadedBookmark.getId());
    }

    public void testShortSupport() throws Exception {

        IgniteCache<String, Bookmark> cache = grid(NODE_CLIENT).cache(CACHE_BOOKMARK);
        Bookmark bookmark = new Bookmark();
        bookmark.setId(UUID.randomUUID().toString());
        bookmark.setUrlPort(Short.MAX_VALUE);
        cache.put(bookmark.id, bookmark);

        Bookmark loadedBookmark = searchBookmarkBy("urlPort", Short.MAX_VALUE);
        assertEquals("Short value does not match", bookmark.getUrlPort(), loadedBookmark.getUrlPort());
        assertEquals("Id value does not match", bookmark.getId(), loadedBookmark.getId());
    }

    public void testLongSupport() throws Exception {

        IgniteCache<String, Bookmark> cache = grid(NODE_CLIENT).cache(CACHE_BOOKMARK);
        Bookmark bookmark = new Bookmark();
        bookmark.setId(UUID.randomUUID().toString());
        bookmark.setUserId(Long.MAX_VALUE);
        cache.put(bookmark.id, bookmark);

        Bookmark loadedBookmark = searchBookmarkBy("userId", Long.MAX_VALUE);
        assertEquals("Long value does not match", bookmark.getUserId(), loadedBookmark.getUserId());
        assertEquals("Id value does not match", bookmark.getId(), loadedBookmark.getId());
    }

    public void testFloatSupport() throws Exception {

        IgniteCache<String, Bookmark> cache = grid(NODE_CLIENT).cache(CACHE_BOOKMARK);
        Bookmark bookmark = new Bookmark();
        bookmark.setId(UUID.randomUUID().toString());
        bookmark.setVisitRatio(Float.MAX_VALUE);
        cache.put(bookmark.id, bookmark);

        Bookmark loadedBookmark = searchBookmarkBy("visitRatio", Float.MAX_VALUE);
        assertEquals("Float value does not match", bookmark.getVisitRatio(), loadedBookmark.getVisitRatio());
        assertEquals("Id value does not match", bookmark.getId(), loadedBookmark.getId());
    }

    public void testDoubleSupport() throws Exception {

        IgniteCache<String, Bookmark> cache = grid(NODE_CLIENT).cache(CACHE_BOOKMARK);
        Bookmark bookmark = new Bookmark();
        bookmark.setId(UUID.randomUUID().toString());
        bookmark.setTaxPercentage(Double.MAX_VALUE);
        cache.put(bookmark.id, bookmark);

        Bookmark loadedBookmark = searchBookmarkBy("taxPercentage", Double.MAX_VALUE);
        assertEquals("Double value does not match", bookmark.getTaxPercentage(), loadedBookmark.getTaxPercentage());
        assertEquals("Id value does not match", bookmark.getId(), loadedBookmark.getId());
    }

    public void testBooleanSupport() throws Exception {

        IgniteCache<String, Bookmark> cache = grid(NODE_CLIENT).cache(CACHE_BOOKMARK);
        Bookmark bookmark = new Bookmark();
        bookmark.setId(UUID.randomUUID().toString());
        bookmark.setFavourite(true);
        cache.put(bookmark.id, bookmark);

        Bookmark loadedBookmark = searchBookmarkBy("favourite", true);
        assertEquals("Boolean value does not match", bookmark.getFavourite(), loadedBookmark.getFavourite());
        assertEquals("Id value does not match", bookmark.getId(), loadedBookmark.getId());
    }

    public void testByteSupport() throws Exception {

        IgniteCache<String, Bookmark> cache = grid(NODE_CLIENT).cache(CACHE_BOOKMARK);
        Bookmark bookmark = new Bookmark();
        bookmark.setId(UUID.randomUUID().toString());
        bookmark.setDisplayMask(Byte.MAX_VALUE);
        cache.put(bookmark.id, bookmark);

        Bookmark loadedBookmark = searchBookmarkBy("displayMask", Byte.MAX_VALUE);
        assertEquals("Byte value does not match", bookmark.getDisplayMask(), loadedBookmark.getDisplayMask());
        assertEquals("Id value does not match", bookmark.getId(), loadedBookmark.getId());
    }

    public void testUUIDSupport() throws Exception {

        IgniteCache<String, Bookmark> cache = grid(NODE_CLIENT).cache(CACHE_BOOKMARK);
        Bookmark bookmark = new Bookmark();
        bookmark.setId(UUID.randomUUID().toString());
        final UUID uuid = UUID.randomUUID();
        bookmark.setSerialNumber(uuid);
        cache.put(bookmark.id, bookmark);

        Bookmark loadedBookmark = searchBookmarkBy("serialNumber", uuid);
        assertEquals("UUID value does not match", bookmark.getSerialNumber(), loadedBookmark.getSerialNumber());
        assertEquals("Id value does not match", bookmark.getId(), loadedBookmark.getId());
    }
    public void testBigIntegerSupport() throws Exception {

        IgniteCache<String, Bookmark> cache = grid(NODE_CLIENT).cache(CACHE_BOOKMARK);
        Bookmark bookmark = new Bookmark();
        bookmark.setId(UUID.randomUUID().toString());
        BigInteger bi = new BigInteger("1000000000000000");
        bookmark.setVisitCount(bi);
        cache.put(bookmark.id, bookmark);

        Bookmark loadedBookmark = searchBookmarkBy("visitCount", bi);
        assertEquals("BigInteger value does not match", bookmark.getVisitCount(), loadedBookmark.getVisitCount());
        assertEquals("Id value does not match", bookmark.getId(), loadedBookmark.getId());
    }
    public void testBigDecimalSupport() throws Exception {

        IgniteCache<String, Bookmark> cache = grid(NODE_CLIENT).cache(CACHE_BOOKMARK);
        Bookmark bookmark = new Bookmark();
        bookmark.setId(UUID.randomUUID().toString());
        BigDecimal bd = new BigDecimal("1000000000000000.001");
        bookmark.setSiteWeight(bd);
        cache.put(bookmark.id, bookmark);

        Bookmark loadedBookmark = searchBookmarkBy("siteWeight", bd);
        assertEquals("BigDecimal value does not match", bookmark.getSiteWeight(), loadedBookmark.getSiteWeight());
        assertEquals("Id value does not match", bookmark.getId(), loadedBookmark.getId());
    }

    /**
     * Object with all types that supported in H2
     * @see <a href="http://www.h2database.com/html/datatypes.html">H2 types</a>
     */

    public static class Bookmark implements Serializable {
        private String id;

        // basic types
        private String description;
        private Integer stockCount;
        private Short urlPort;
        private Long userId;
        private Float visitRatio;
        private Double taxPercentage;
        private Boolean favourite;
        private Byte displayMask;

        // "special" types
        //http://www.h2database.com/html/datatypes.html#uuid_type
        private UUID serialNumber;
        private BigDecimal siteWeight;
        private BigInteger visitCount;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }


        public Integer getStockCount() {
            return stockCount;
        }

        public void setStockCount(Integer stockCount) {
            this.stockCount = stockCount;
        }

        public Short getUrlPort() {
            return urlPort;
        }

        public void setUrlPort(Short urlPort) {
            this.urlPort = urlPort;
        }

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public Float getVisitRatio() {
            return visitRatio;
        }

        public void setVisitRatio(Float visitRatio) {
            this.visitRatio = visitRatio;
        }

        public Double getTaxPercentage() {
            return taxPercentage;
        }

        public void setTaxPercentage(Double taxPercentage) {
            this.taxPercentage = taxPercentage;
        }

        public Boolean getFavourite() {
            return favourite;
        }

        public void setFavourite(Boolean favourite) {
            this.favourite = favourite;
        }

        public Byte getDisplayMask() {
            return displayMask;
        }

        public void setDisplayMask(Byte displayMask) {
            this.displayMask = displayMask;
        }

        public UUID getSerialNumber() {
            return serialNumber;
        }

        public void setSerialNumber(UUID serialNumber) {
            this.serialNumber = serialNumber;
        }

        public BigDecimal getSiteWeight() {
            return siteWeight;
        }

        public void setSiteWeight(BigDecimal siteWeight) {
            this.siteWeight = siteWeight;
        }

        public BigInteger getVisitCount() {
            return visitCount;
        }

        public void setVisitCount(BigInteger visitCount) {
            this.visitCount = visitCount;
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Bookmark bookmark = (Bookmark)o;
            return Objects.equals(id, bookmark.id);
        }

        @Override public int hashCode() {
            return Objects.hash(id);
        }
    }

}


