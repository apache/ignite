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

package org.apache.ignite.internal.processors.query;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test sql queries with parameters for all types.
 * The test is fix  for issue 'IGNITE-6286'
 *
 * @author Sergey Chernolyas &amp;sergey_chernolyas@gmail.com&amp;
 * @see <a href="https://issues.apache.org/jira/browse/IGNITE-6286">IGNITE-6286</a>
 */
public class IgniteSqlParameterizedQueryTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);


    private static final String CACHE_BOOKMARK = "Bookmark";
    private static final String NODE_CLIENT = "client";

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


    /**
     * build cache configuration
     * @param name cache name
     * @return configuration
     * @see CacheConfiguration
     */
    private CacheConfiguration buildCacheConfiguration(String name) {
        CacheConfiguration ccfg = new CacheConfiguration(name);
        ccfg.setIndexedTypes(String.class, Bookmark.class);
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


    /**
     * method for create parametrized query and get first result
     * @param field name of field
     * @param value value
     * @return fist searched object
     * @see Bookmark
     */
    private Bookmark searchBookmarkBy(String field, Object value) {
        IgniteCache<String, Bookmark> cache = grid(NODE_CLIENT).cache(CACHE_BOOKMARK);
        SqlFieldsQuery query = new SqlFieldsQuery("SELECT _val from  Bookmark where "+field+"=?");
        query.setArgs(value);

        QueryCursor<List<?>> cursor = cache.query(query);
        List<List<?>> results = cursor.getAll();
        assertEquals("Find by field '"+field+"' returns incorrect row count!",1, results.size());
        List<?> row0 = results.get(0);
        return (Bookmark)row0.get(0);
    }

    /**
     * check common fields
     * @param bookmark source bookmark
     * @param loadedBookmark bookmark loaded from Ignite
     */
    private void checkCommonFields(Bookmark bookmark, Bookmark loadedBookmark) {
        assertEquals("Id value does not match", bookmark.getId(), loadedBookmark.getId());
    }

    /**
     * testing parametrized query by field with supported type
     * @throws Exception if any error occurs
     */
    public void testSupportedTypes() throws Exception {
        IgniteCache<String, Bookmark> cache = grid(NODE_CLIENT).cache(CACHE_BOOKMARK);
        Bookmark bookmark = new Bookmark();
        bookmark.setId(UUID.randomUUID().toString());
        bookmark.setDescription("description");
        bookmark.setStockCount(Integer.MAX_VALUE);
        bookmark.setUrlPort(Short.MAX_VALUE);
        bookmark.setUserId(Long.MAX_VALUE);
        bookmark.setVisitRatio(Float.MAX_VALUE);
        bookmark.setTaxPercentage(Double.MAX_VALUE);
        bookmark.setFavourite(true);
        bookmark.setDisplayMask(Byte.MAX_VALUE);
        bookmark.setSerialNumber(UUID.randomUUID());
        bookmark.setVisitCount(new BigInteger("1000000000000000"));
        bookmark.setSiteWeight(new BigDecimal("1000000000000000.001"));
        bookmark.setCreated(new Date());
        cache.put(bookmark.id, bookmark);

        for (Field currentField : Bookmark.class.getDeclaredFields()) {

            Method getter = Bookmark.class.getMethod("get"+ StringUtils.capitalize(currentField.getName()));
            Object value = getter.invoke(bookmark);

            Bookmark loadedBookmark = searchBookmarkBy(currentField.getName(), value);
            Object loadedValue = getter.invoke(loadedBookmark);
            assertEquals(value.getClass().getSimpleName()+" value does not match",value, loadedValue);
            checkCommonFields(bookmark, loadedBookmark);
        }
    }


    /**
     * Object with all predefined SQL Data Types
     * @see <a href="https://apacheignite.readme.io/docs/dml#section-advanced-configuration">Predefined SQL Data Types</a>
     */

    public static class Bookmark implements Serializable {
        @QuerySqlField
        private String id;

        // basic types
        @QuerySqlField
        private String description;
        @QuerySqlField
        private Integer stockCount;
        @QuerySqlField
        private Short urlPort;
        @QuerySqlField
        private Long userId;
        @QuerySqlField
        private Float visitRatio;
        @QuerySqlField
        private Double taxPercentage;
        @QuerySqlField
        private Boolean favourite;
        @QuerySqlField
        private Byte displayMask;

        // "special" types
        @QuerySqlField
        private UUID serialNumber;
        @QuerySqlField
        private BigDecimal siteWeight;
        @QuerySqlField
        private BigInteger visitCount;

        //data types
        @QuerySqlField
        private Date created;

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

        public Date getCreated() {
            return created;
        }

        public void setCreated(Date created) {
            this.created = created;
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


