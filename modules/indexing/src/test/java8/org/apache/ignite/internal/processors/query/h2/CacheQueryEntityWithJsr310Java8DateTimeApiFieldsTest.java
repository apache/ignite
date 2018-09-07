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
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Tests queries against entities with JSR-310 Java 8 Date and Time API fields.
 */
public class CacheQueryEntityWithJsr310Java8DateTimeApiFieldsTest extends CacheQueryJsr310Java8DateTimeApiAbstractTest {
    /**
     * Entity containing JSR-310 fields.
     */
    private static class EntityWithJsr310Fields implements Serializable {

        /** Serial version UID. */
        private static final long serialVersionUID = 1L;

        /** ID. */
        @QuerySqlField(index = true)
        private Long id;

        /** {@link LocalTime} field. */
        @QuerySqlField(index = true)
        private LocalTime locTime;

        /** {@link LocalDate} field. */
        @QuerySqlField(index = true)
        private LocalDate locDate;

        /** {@link LocalDateTime} field. */
        @QuerySqlField(index = true)
        private LocalDateTime locDateTime;

        /**
         * Default constructor.
         */
        EntityWithJsr310Fields() {
        }

        /**
         * Copy constructor.
         *
         * @param entity Entity to copy from.
         */
        EntityWithJsr310Fields(EntityWithJsr310Fields entity) {
            id = entity.id;
            locTime = LocalTime.from(entity.locTime);
            locDate = LocalDate.from(entity.locDate);
            locDateTime = LocalDateTime.from(entity.locDateTime);
        }

        /**
         * Constructor.
         *
         * @param id ID.
         * @param locTime {@link LocalTime} value.
         * @param locDate {@link LocalDate} value.
         * @param locDateTime {@link LocalDateTime} value.
         */
        EntityWithJsr310Fields(Long id, LocalTime locTime, LocalDate locDate, LocalDateTime locDateTime) {
            this.id = id;
            this.locTime = locTime;
            this.locDate = locDate;
            this.locDateTime = locDateTime;
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
         * Returns the {@link LocalDateTime} field value
         *
         * @return {@link LocalDateTime} field value;
         */
        public LocalDateTime getLocalDateTime() {
            return locDateTime;
        }

        /**
         * Returns the {@link LocalDateTime} field value.
         *
         * @param locDateTime {@link LocalDateTime} value.
         */
        public void setLocalDateTime(LocalDateTime locDateTime) {
            this.locDateTime = locDateTime;
        }

        /**
         * Returns the {@link LocalDate} field value.
         *
         * @return {@link LocalDate} field value.
         */
        public LocalDate getLocalDate() {
            return locDate;
        }

        /**
         * Sets the {@link LocalDate} field value.
         *
         * @param locDate {@link LocalDate} value.
         */
        public void setLocalDate(LocalDate locDate) {
            this.locDate = locDate;
        }

        /**
         * Returns the {@link LocalTime} field value.
         *
         * @return {@link LocalTime} field value.
         */
        public LocalTime getLocalTime() {
            return locTime;
        }

        /**
         * Sets the {@link LocalTime} field value.
         *
         * @param locTime {@link LocalTime} value.
         */
        public void setLocalTime(LocalTime locTime) {
            this.locTime = locTime;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            EntityWithJsr310Fields fields = (EntityWithJsr310Fields)o;

            return Objects.equals(id, fields.id) && Objects.equals(locDateTime, fields.locDateTime) &&
                Objects.equals(locDate, fields.locDate) && Objects.equals(locTime, fields.locTime);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, locDateTime, locDate, locTime);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "EntityWithJsr310Fields{" + "id=" + id + ", locDateTime=" + locDateTime + ", locDate=" + locDate +
                ", locTime=" + locTime + '}';
        }
    }

    /** Cache. */
    private IgniteCache<Long, EntityWithJsr310Fields> cache;

    /** Entity with JSR-310 fields instance. */
    private final EntityWithJsr310Fields entity =
        new EntityWithJsr310Fields(1L, LOCAL_TIME, LOCAL_DATE, LOCAL_DATE_TIME);

    /**
     * Creates a cache configuration.
     *
     * @return Cache configuration.
     */
    private static CacheConfiguration<Long, EntityWithJsr310Fields> createCacheConfig() {
        return createCacheConfig(
            "entityWithJava8DataTimeFields", Long.class, EntityWithJsr310Fields.class
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Ignite ignite = startGridsMultiThreaded(1, true);
        cache = ignite.getOrCreateCache(createCacheConfig());

        cache.put(entity.getId(), entity);
    }

    /**
     * Tests insertion of an entity.
     *
     * @throws Exception If failed.
     */
    public void testInsertEntityFields() throws Exception {
        cache.remove(entity.getId());

        assertEquals(0, cache.size());

        SqlFieldsQuery qry = new SqlFieldsQuery(
            "insert into EntityWithJsr310Fields(_key, id, locTime, locDate, locDateTime) values(?, ?, ?, ?, ?)"
        ).setArgs(
            entity.getId(), entity.getId(), entity.getLocalTime(), entity.getLocalDate(), entity.getLocalDateTime()
        );

        List<List<?>> qryResults = cache.query(qry).getAll();

        assertEquals(1, qryResults.size());
        assertEquals(1L, qryResults.get(0).get(0));
        assertEquals(1, cache.size());
        assertEquals(entity, cache.get(entity.getId()));
    }

    /**
     * Tests that DATEDIFF SQL function works for {@link LocalDateTime}
     * fields with the time part set to midnight.
     *
     * @throws Exception If failed.
     */
    public void testDateDiffForLocalDateTimeFieldAtMidnight() throws Exception {
        SqlFieldsQuery qry =
            new SqlFieldsQuery("select DATEDIFF('DAY', locDateTime, CURRENT_DATE ()) from EntityWithJsr310Fields");

        List<List<?>> qryResults = cache.query(qry).getAll();

        assertEquals(1, qryResults.size());
        assertTrue((Long)qryResults.get(0).get(0) >= DAYS_BEFORE_NOW);
    }

    /**
     * Tests that selection for a {@link LocalTime} field returns {@link Time}.
     *
     * @throws Exception If failed.
     */
    public void testSelectLocalTimeFieldReturnsTime() throws Exception {
        SqlFieldsQuery qry = new SqlFieldsQuery("select locTime from EntityWithJsr310Fields");

        List<List<?>> qryResults = cache.query(qry).getAll();

        assertEquals(1, qryResults.size());
        assertEquals(Time.class, qryResults.get(0).get(0).getClass());
    }

    /**
     * Tests that selection for a {@link LocalDate} field returns {@link Date}.
     *
     * @throws Exception If failed.
     */
    public void testSelectLocalDateFieldReturnsDate() throws Exception {
        SqlFieldsQuery qry = new SqlFieldsQuery("select locDate from EntityWithJsr310Fields");

        List<List<?>> qryResults = cache.query(qry).getAll();

        assertEquals(1, qryResults.size());
        assertEquals(Date.class, qryResults.get(0).get(0).getClass());
    }

    /**
     * Tests that selection for a {@link LocalDateTime} field returns {@link Timestamp}.
     *
     * @throws Exception If failed.
     */
    public void testSelectLocalDateTimeFieldReturnsTimestamp() throws Exception {
        SqlFieldsQuery qry = new SqlFieldsQuery("select locDateTime from EntityWithJsr310Fields");

        List<List<?>> qryResults = cache.query(qry).getAll();

        assertEquals(1, qryResults.size());
        assertEquals(Timestamp.class, qryResults.get(0).get(0).getClass());
    }

    /**
     * Tests selection of an entity by a {@link LocalTime} field.
     */
    public void testSelectByAllJsr310Fields() {
        SqlFieldsQuery qry = new SqlFieldsQuery(
            "select locDate from EntityWithJsr310Fields where locTime = ? and locDate = ? and locDateTime = ?"
        ).setArgs(entity.getLocalTime(), entity.getLocalDate(), entity.getLocalDateTime());

        List<List<?>> qryResults = cache.query(qry).getAll();

        assertEquals(1, qryResults.size());
        assertEquals(Date.valueOf(entity.getLocalDate()), qryResults.get(0).get(0));
    }

    /**
     * Tests updating of all JSR-310 fields.
     */
    public void testUpdateAllJsr310Fields() {
        EntityWithJsr310Fields expEntity = new EntityWithJsr310Fields(entity);

        expEntity.setLocalTime(expEntity.getLocalTime().plusHours(1));
        expEntity.setLocalDate(expEntity.getLocalDate().plusDays(1));
        expEntity.setLocalDateTime(LocalDateTime.of(expEntity.getLocalDate(), expEntity.getLocalTime()));

        SqlFieldsQuery qry = new SqlFieldsQuery(
            "update EntityWithJsr310Fields set locTime = ?, locDate = ?, locDateTime = ? where id = ?"
        ).setArgs(expEntity.getLocalTime(), expEntity.getLocalDate(), expEntity.getLocalDateTime(), entity.getId());

        List<List<?>> qryResults = cache.query(qry).getAll();

        assertEquals(1, qryResults.size());
        assertEquals(1L, qryResults.get(0).get(0));
        assertEquals(expEntity, cache.get(expEntity.getId()));
    }

    /**
     * Tests deleting by all JSR-310 fields.
     */
    public void testDeleteByAllJsr310Fields() {
        SqlFieldsQuery qry = new SqlFieldsQuery(
            "delete from EntityWithJsr310Fields where locTime = ? and locDate = ? and locDateTime = ?"
        ).setArgs(entity.getLocalTime(), entity.getLocalDate(), entity.getLocalDateTime());

        List<List<?>> qryResults = cache.query(qry).getAll();

        assertEquals(1, qryResults.size());
        assertEquals(1L, qryResults.get(0).get(0));
        assertEquals(0, cache.size());
    }
}
