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

package org.apache.ignite.internal.processors.query.calcite.integration;

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
import java.util.function.Function;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.convertToSqlDate;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.convertToSqlTime;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateValueUtils.convertToTimestamp;

/** */
@RunWith(Parameterized.class)
public class LocalDateTimeSupportTest extends AbstractBasicIntegrationTest {
    /** */
    @Parameterized.Parameter()
    public boolean isValidationEnabled;

    /** */
    @Parameterized.Parameter(1)
    public String sqlType;

    /** */
    @Parameterized.Parameter(2)
    public Class<?> colType;

    /** */
    @Parameterized.Parameter(3)
    public Class<?> objType;

    /** */
    @Parameterized.Parameter(4)
    public Function<Object, Object> sqlTypeConverter;

    /** */
    @Parameterized.Parameters(name = "isValidationEnabled={0}, sqlType={1}, columnCls={2}, testObjCls={3}")
    public static Collection<Object[]> parameters() {
        Collection<Object[]> params = new ArrayList<>();

        for (boolean isV : Arrays.asList(true, false)) {
            params.add(new Object[] {
                isV, "TIMESTAMP", null, LocalDateTime.class, f(ts -> convertToTimestamp((LocalDateTime)ts))
            });
            params.add(new Object[] {
                isV, "TIMESTAMP", null, Date.class, f(ts -> new Timestamp(((Date)ts).getTime()))
            });
            params.add(new Object[] {
                isV, "TIMESTAMP", null, java.sql.Date.class, f(ts -> new Timestamp(((Date)ts).getTime()))
            });

            for (Class<?> testObjCls : Arrays.asList(Timestamp.class, LocalDateTime.class, Date.class, java.sql.Date.class)) {
                params.add(new Object[] {
                    isV, null, Timestamp.class, testObjCls, f(ts -> {
                        if (ts instanceof LocalDateTime)
                            return convertToTimestamp((LocalDateTime)ts);
                        return ts;
                    })
                });

                params.add(new Object[] {
                    isV, null, Date.class, testObjCls, f(ts -> {
                        if (testObjCls == LocalDateTime.class)
                            return new Date(convertToTimestamp((LocalDateTime)ts).getTime());
                        else if (testObjCls == Timestamp.class)
                            return new Date(((Timestamp)ts).getTime());
                        return ts;
                    })
                });

                params.add(new Object[] {
                    isV, null, LocalDateTime.class, testObjCls, f(ts -> {
                        if (testObjCls == Timestamp.class)
                            return ((Timestamp)ts).toLocalDateTime();
                        else if (testObjCls == java.util.Date.class)
                            return new Timestamp(((Date)ts).getTime()).toLocalDateTime();
                        else if (testObjCls == java.sql.Date.class)
                            return ((java.sql.Date)ts).toLocalDate().atStartOfDay();
                        else
                            return ts;
                    })
                });
            }

            params.add(new Object[] {
                isV, "DATE", null, LocalDate.class, f(d -> convertToSqlDate((LocalDate)d))
            });

            for (Class<?> testObjCls : Arrays.asList(LocalDate.class, java.sql.Date.class)) {
                params.add(new Object[] {
                    isV, null, java.sql.Date.class, testObjCls, f(ts -> {
                        if (testObjCls == LocalDate.class)
                            return convertToSqlDate((LocalDate)ts);
                        return ts;
                    })
                });

                params.add(new Object[] {
                    isV, null, LocalDate.class, testObjCls, f(ts -> {
                        if (testObjCls == java.sql.Date.class)
                            return ((java.sql.Date)ts).toLocalDate();
                        return ts;
                    })
                });
            }

            params.add(new Object[] {
                isV, "TIME", null, LocalTime.class, f(t -> convertToSqlTime((LocalTime)t))
            });

            for (Class<?> testObjCls : Arrays.asList(LocalTime.class, java.sql.Time.class)) {
                params.add(new Object[] {
                    isV, null, java.sql.Time.class, testObjCls, f(ts -> {
                        if (testObjCls == LocalTime.class)
                            return convertToSqlTime((LocalTime)ts);
                        return ts;
                    })
                });

                params.add(new Object[] {
                    isV, null, LocalTime.class, testObjCls, f(ts -> {
                        if (testObjCls == java.sql.Time.class)
                            return ((java.sql.Time)ts).toLocalTime();
                        return ts;
                    })
                });
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
    @Override public void beforeTest() throws Exception {
        if (!G.allGrids().isEmpty())
            return;

        startGrids(nodeCount());

        client = startClientGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testTemporalTypes() {
        createTable();

        executeSql("CREATE INDEX DATA_IDX ON DATA(data DESC);");

        Object testObj = generateTestObject(objType);

        executeSql("INSERT INTO DATA(_key, id, data) values(?, ?, ?)", 0, 0, testObj);

        grid(0).cache(DEFAULT_CACHE_NAME).put(1, new Data(1, testObj));

        grid(0).cache(DEFAULT_CACHE_NAME).put(2, grid(0).binary().toBinary(new Data(2, testObj)));

        List<List<?>> selectData = executeSql("SELECT data FROM DATA");

        Object sqlObj = sqlTypeConverter.apply(testObj);

        selectData.get(0).forEach(d -> assertEquals(sqlObj, d));
    }

    /** */
    private void createTable() {
        if (sqlType != null) {
            executeSql("CREATE TABLE DATA (id INT PRIMARY KEY, data " + sqlType + ") WITH" +
                " \"KEY_TYPE=java.lang.Integer" +
                ", VALUE_TYPE=" + Data.class.getName() +
                ", CACHE_NAME=default\"");
        }
        else {
            QueryEntity projEntity = new QueryEntity();
            projEntity.setKeyType(Integer.class.getName());
            projEntity.setValueType(Data.class.getName());
            projEntity.addQueryField("id", Integer.class.getName(), null);
            projEntity.addQueryField("data", colType.getName(), null);

            projEntity.setTableName("DATA");

            grid(0).createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setQueryEntities(singletonList(projEntity))
                .setSqlSchema("PUBLIC"));
        }
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
        else if (cls == java.sql.Timestamp.class)
            return java.sql.Timestamp.valueOf(LocalDateTime.now());
        else
            throw new IllegalStateException();
    }

    /** */
    private static <T, R> Function<Object, Object> f(Function<T, R> f) {
        return (Function<Object, Object>)f;
    }

    /** */
    public static class Data {
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
