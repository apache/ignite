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
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Collections.singletonList;
import static org.h2.util.LocalDateTimeUtils.localDateToDateValue;
import static org.h2.util.LocalDateTimeUtils.localTimeToTimeValue;

/** */
@RunWith(Parameterized.class)
public class IgniteQueryConvertibleTypesValidationTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter()
    public boolean isValidationEnabled;

    /** */
    @Parameterized.Parameter(1)
    public String sqlType;

    /** */
    @Parameterized.Parameter(2)
    public Class<?> objType;

    /** */
    @Parameterized.Parameter(3)
    public Function<Object, Object> sqlTypeConverter;

    /** */
    @Parameterized.Parameter(4)
    public boolean isDdl;

    /** */
    @Parameterized.Parameters(name = "isValidationEnabled={0}, sqlType={1}, testObjCls={2}, isDdl={4}")
    public static Collection<Object[]> parameters() {
        Collection<Object[]> params = new ArrayList<>();

        for (boolean isV : Arrays.asList(true, false)) {
            for (boolean isDdl : Arrays.asList(true, false)) {
                params.add(new Object[] {isV, "TIMESTAMP", LocalDateTime.class, f(d -> Timestamp.valueOf((LocalDateTime)d)), isDdl});
                params.add(new Object[] {isV, "TIMESTAMP", Date.class, f(d -> new Timestamp(((Date)d).getTime())), isDdl});
                params.add(new Object[] {isV, "TIMESTAMP", java.sql.Date.class, f(d -> new Timestamp(((Date)d).getTime())), isDdl});
                params.add(new Object[] {isV, "DATE", LocalDate.class, f(d -> localDateToDateValue(d).getDate()), isDdl});
                params.add(new Object[] {isV, "TIME", LocalTime.class, f(d -> localTimeToTimeValue(d).getTime()), isDdl});
            }
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getSqlConfiguration().setValidationEnabled(isValidationEnabled);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testIgniteQueryConvertibleTypesValidation() throws Exception {
        startGrid(0);

        createTable();

        execute("CREATE INDEX DATA_IDX ON DATA(data DESC);");

        Object testObj = generateTestObject(objType);

        execute("INSERT INTO DATA(_key, id, data) values(?, ?, ?)", 0, 0, testObj);

        grid(0).cache(DEFAULT_CACHE_NAME).put(1, new Data(1, testObj));

        grid(0).cache(DEFAULT_CACHE_NAME).put(2, grid(0).binary().toBinary(new Data(2, testObj)));

        List<List<?>> selectData = execute("SELECT data FROM DATA");

        Object sqlObj = sqlTypeConverter.apply(testObj);

        assertTrue(selectData.get(0).stream().allMatch(d -> Objects.equals(sqlObj, d)));
    }

    /** */
    private void createTable() {
        if (isDdl) {
            execute("CREATE TABLE DATA (id INT PRIMARY KEY, data " + sqlType + ") WITH" +
                " \"KEY_TYPE=java.lang.Integer" +
                ", VALUE_TYPE=org.apache.ignite.internal.processors.query.IgniteQueryConvertibleTypesValidationTest$Data" +
                ", CACHE_NAME=default\"");
        }
        else {
            QueryEntity projEntity = new QueryEntity();
            projEntity.setKeyType(Integer.class.getName());
            projEntity.setValueType(Data.class.getName());
            projEntity.addQueryField("id", Integer.class.getName(), null);
            projEntity.addQueryField("data", toClassName(sqlType), null);

            projEntity.setTableName("DATA");

            grid(0).createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setQueryEntities(singletonList(projEntity))
                .setSqlSchema("PUBLIC"));
        }
    }

    /** */
    private List<List<?>> execute(String qry, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(qry).setArgs(args), false).getAll();
    }

    /** */
    private static Object generateTestObject(Class<?> cls) {
        if (cls == LocalDateTime.class)
            return LocalDateTime.now();
        else if (cls == LocalTime.class)
            return LocalTime.now();
        else if (cls == LocalDate.class)
            return LocalDate.now();
        else if (cls == Date.class)
            return Date.from(Instant.now());
        else if (cls == java.sql.Date.class)
            return java.sql.Date.valueOf(LocalDate.now());
        else if (cls == java.sql.Time.class)
            return java.sql.Time.valueOf(LocalTime.now());
        else
            throw new IllegalStateException();
    }

    /** */
    private static String toClassName(String sqlType) {
        switch (sqlType) {
            case "TIMESTAMP" : return java.sql.Timestamp.class.getName();
            case "TIME" : return java.sql.Time.class.getName();
            case "DATE" : return java.sql.Date.class.getName();
            default: throw new IllegalStateException();
        }
    }

    /** */
    private static <T, R> Function<Object, Object> f(Function<T, R> f) {
        return (Function<Object, Object>)f;
    }

    /** */
    public static class Data implements Serializable {
        /** Serial version UID. */
        private static final long serialVersionUID = 1L;

        /** */
        public int id;

        /** */
        public Object data;

        /** */
        public Data(int id, Object data) {
            this.id = id;
            this.data = data;
        }
    }
}
