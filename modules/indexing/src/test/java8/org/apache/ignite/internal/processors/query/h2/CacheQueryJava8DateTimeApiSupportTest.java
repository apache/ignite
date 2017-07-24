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

package org.apache.ignite.internal.processors.query.h2;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests JSR-310 Java 8 Date and Time API types queries support.
 */
@SuppressWarnings("unchecked")
public class CacheQueryJava8DateTimeApiSupportTest extends GridCommonAbstractTest {
    /**
     * Entity containing fields of Java 8 Date and Time API types.
     */
    private static class EntityWithJava8DateTimeApiFields implements Serializable {

        /** Serial version uid. */
        private static final long serialVersionUID = 1L;

        /** ID. */
        @QuerySqlField(index = true)
        private Long id;

        /** {@link java.time.LocalDateTime} field. */
        @QuerySqlField(index = true)
        private LocalDateTime locDateTime;

        /** {@link java.time.LocalDate} field. */
        @QuerySqlField(index = true)
        private LocalDate locDate;

        /** {@link java.time.LocalTime} field. */
        @QuerySqlField(index = true)
        private LocalTime locTime;

        /**
         * Default constructor.
         */
        public EntityWithJava8DateTimeApiFields() {
        }

        /**
         * Constructor.
         *
         * @param id ID.
         * @param locDateTime {@link java.time.LocalDateTime} value.
         * @param locDate {@link java.time.LocalDate} value.
         * @param locTime {@link java.time.LocalTime} value.
         */
        public EntityWithJava8DateTimeApiFields(Long id, LocalDateTime locDateTime, LocalDate locDate,
            LocalTime locTime) {
            this.id = id;
            this.locDateTime = locDateTime;
            this.locDate = locDate;
            this.locTime = locTime;
        }

        /**
         * Returns the ID.
         *
         * @return ID.
         */
        public Long getId() {
            return id;
        }

        /**
         * Sets the ID.
         *
         * @param id ID.
         */
        public void setId(Long id) {
            this.id = id;
        }

        /**
         * Returns the {@link java.time.LocalDateTime} field value
         *
         * @return {@link java.time.LocalDateTime} field value;
         */
        public LocalDateTime getLocalDateTime() {
            return locDateTime;
        }

        /**
         * Returns the {@link java.time.LocalDateTime} field value.
         *
         * @param locDateTime {@link java.time.LocalDateTime} value.
         */
        public void setLocalDateTime(LocalDateTime locDateTime) {
            this.locDateTime = locDateTime;
        }

        /**
         * Returns the {@link java.time.LocalDate} field value.
         *
         * @return {@link java.time.LocalDate} field value.
         */
        public LocalDate getLocDate() {
            return locDate;
        }

        /**
         * Sets the {@link java.time.LocalDate} field value.
         *
         * @param locDate {@link java.time.LocalDate} value.
         */
        public void setLocDate(LocalDate locDate) {
            this.locDate = locDate;
        }

        /**
         * Returns the {@link java.time.LocalTime} field value.
         *
         * @return {@link java.time.LocalTime} field value.
         */
        public LocalTime getLocTime() {
            return locTime;
        }

        /**
         * Sets the {@link java.time.LocalTime} field value.
         *
         * @param locTime {@link java.time.LocalTime} value.
         */
        public void setLocTime(LocalTime locTime) {
            this.locTime = locTime;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            EntityWithJava8DateTimeApiFields fields = (EntityWithJava8DateTimeApiFields)o;

            return Objects.equals(id, fields.id) && Objects.equals(locDateTime, fields.locDateTime) &&
                Objects.equals(locDate, fields.locDate) && Objects.equals(locTime, fields.locTime);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, locDateTime, locDate, locTime);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "EntityWithJava8DateTimeApiFields{" + "id=" + id + ", locDateTime=" + locDateTime +
                ", locDate=" + locDate + ", locTime=" + locTime + '}';
        }
    }

    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Cache for {@link EntityWithJava8DateTimeApiFields}. */
    private IgniteCache<Long, EntityWithJava8DateTimeApiFields> cache;

    /** Entity. */
    private final EntityWithJava8DateTimeApiFields entity = new EntityWithJava8DateTimeApiFields(
        1L,
        LocalDateTime.of(LocalDate.now().minusDays(10), LocalTime.MIDNIGHT),
        LocalDate.now().minusDays(10),
        LocalTime.now().minusHours(10)
    );

    /**
     * Returns a cache configuration with for specified cache name.
     *
     * @param name Cache name.
     * @return Cache configuration.
     */
    private static CacheConfiguration<Long, EntityWithJava8DateTimeApiFields> cacheConfig(String name) {
        return new CacheConfiguration()
            .setName(name)
            .setCacheMode(CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setIndexedTypes(Long.class, EntityWithJava8DateTimeApiFields.class);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Ignite ignite = startGridsMultiThreaded(1, true);

        final String cacheName = "entityWithJava8DataTimeFields";

        cache = ignite.getOrCreateCache(cacheConfig(cacheName));
        cache.put(entity.getId(), entity);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Tests whether Java 8 Date and Time API types are supported.
     *
     * @throws Exception If failed.
     */
    public void testJava8DateTimeApiTimeSupported() throws Exception {
        SqlFieldsQuery qry = new SqlFieldsQuery("select _val from EntityWithJava8DateTimeApiFields");

        List<List<?>> qryResults = cache.query(qry).getAll();

        assertEquals(1, qryResults.size());
        assertEquals(entity, qryResults.get(0).get(0));
    }

    /**
     * Tests whether DATEDIFF SQL function works for {@link java.time.LocalDateTime} types with the time part set to
     * midnight.
     *
     * @throws Exception If failed.
     */
    public void testDateDiffForLocalDateTimeFieldAtMidnight() throws Exception {
        SqlFieldsQuery qry = new SqlFieldsQuery(
            "select DATEDIFF('DAY', locDateTime, CURRENT_DATE ()) from EntityWithJava8DateTimeApiFields");

        List<List<?>> qryResults = cache.query(qry).getAll();

        assertEquals(1, qryResults.size());
        assertEquals(10L, qryResults.get(0).get(0));
    }
}
