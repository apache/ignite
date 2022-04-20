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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
    @Parameterized.Parameters(name = "isValidationEnabled={0}, sqlType={1}, testObjCls={2}")
    public static Collection<Object[]> parameters() {
        Collection<Object[]> params = new ArrayList<>();

        for (boolean v : Arrays.asList(true, false)) {
            params.add(new Object[] {v, "TIMESTAMP", LocalDateTime.class, f(d -> Timestamp.valueOf((LocalDateTime)d))});
            params.add(new Object[] {v, "TIMESTAMP", Date.class, f(d -> new Timestamp(((Date)d).getTime()))});
            params.add(new Object[] {v, "TIMESTAMP", java.sql.Date.class, f(d -> new Timestamp(((Date)d).getTime()))});
            params.add(new Object[] {v, "DATE", LocalDate.class, f(d -> localDateToDateValue(d).getDate())});
            params.add(new Object[] {v, "TIME", LocalTime.class, f(d -> localTimeToTimeValue(d).getTime())});
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        execute("DROP TABLE IF EXISTS DATA;");

        grid(0).destroyCache(DEFAULT_CACHE_NAME);

        grid(0).binary().types().forEach(type -> grid(0).context().cacheObjects().removeType(type.typeId()));
    }

    /** */
    @Test
    public void testIgniteQueryConvertibleTypesValidation() throws Exception {
        execute("CREATE TABLE DATA (id INT PRIMARY KEY, data " + sqlType + ") WITH" +
            " \"KEY_TYPE=java.lang.Integer" +
            ", VALUE_TYPE=org.apache.ignite.internal.processors.query.IgniteQueryConvertibleTypesValidationTest$Data" +
            ", CACHE_NAME=default\"");

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
    private static <T, R> Function<Object, Object> f(Function<T, R> f) {
        return (Function<Object, Object>)f;
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
    private List<List<?>> execute(String qry, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(qry).setArgs(args), false).getAll();
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
