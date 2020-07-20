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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class SqlFieldTypeValidationTypesTest extends AbstractIndexingCommonTest {
    /** */
    private static final String ERROR = "Type for a column 'NAME' is not compatible with table definition.";

    /** */
    @Parameterized.Parameter(0)
    public Class<?> fieldType;

    /** */
    @Parameterized.Parameter(1)
    public Object okVal;

    /** */
    @Parameterized.Parameter(2)
    public Object errVal;

    /** */
    @Parameterized.Parameter(3)
    public boolean indexed;

    /** */
    private static boolean validate;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getSqlConfiguration().setValidationEnabled(validate);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setQueryEntities(
                Collections.singletonList(
                        new QueryEntity()
                                .setKeyFieldName("id")
                                .setValueType("Person")
                                .setFields(new LinkedHashMap<>(
                                        F.asMap("id", "java.lang.Integer",
                                                "name", fieldType.getName())))
                                .setIndexes(indexed ? Collections.singletonList(new QueryIndex("name")) : null)
        )));

        cfg.setBinaryConfiguration(new BinaryConfiguration().setNameMapper(new BinaryBasicNameMapper(true)));

        return cfg;
    }

    /** */
    @Parameterized.Parameters(name = "{index} type={0} idx={3}")
    public static List<Object[]> data() {
        List<Object[]> res = new ArrayList<>();

        // Type, ok value, error value, indexed flag.
        res.add(new Object[] {Integer.class, 1, "1", false});
        res.add(new Object[] {Integer.class, 1, "1", true});

        res.add(new Object[] {String.class, "1", 1, false});
        res.add(new Object[] {String.class, "1", 1, true});

        res.add(new Object[] {UUID.class, UUID.randomUUID(), UUID.randomUUID().toString(), false});
        res.add(new Object[] {UUID.class, UUID.randomUUID(), UUID.randomUUID().toString(), true});

        res.add(new Object[] {TestEnum.class, TestEnum.A, "C", false});
        res.add(new Object[] {TestEnum.class, TestEnum.A, "C", true});

        res.add(new Object[] {Integer[].class, new Integer[] {0,1}, new String[] {"0", "1"}, false});
        res.add(new Object[] {Integer[].class, new Integer[] {0,1}, new String[] {"0", "1"}, true});

        res.add(new Object[] {int[].class, new Integer[] {0,1}, new String[] {"0", "1"}, false});
        res.add(new Object[] {int[].class, new Integer[] {0,1}, new String[] {"0", "1"}, true});

        res.add(new Object[] {int[].class, new Object[] {0,1}, new Object[] {"0", "1"}, false});
        res.add(new Object[] {int[].class, new Object[] {0,1}, new Object[] {"0", "1"}, true});

        return res;
    }

    /** */
    @After
    public void tearDown() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testError() throws Exception {
        validate = true;

        startGrid(0);

        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        BinaryObject obj = grid(0).binary().builder("Person")
                .setField("id", 1)
                .setField("name", errVal)
                .build();

        GridTestUtils.assertThrows(log, () -> cache.put(1, obj), IgniteException.class, ERROR);

        assertNull(cache.withKeepBinary().get(1));
    }

    /** */
    @Test
    public void testClassInstanceError() throws Exception {
        validate = true;

        startGrid(0);

        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        GridTestUtils.assertThrows(log, () -> cache.put(1, new Person(errVal)), IgniteException.class, ERROR);

        assertNull(cache.withKeepBinary().get(1));
    }

    /** */
    private boolean expectIndexFail() {
        return indexed && (UUID.class == fieldType || Integer.class == fieldType);
    }

    /** */
    public static class Person {
        /** */
        private final Object name;

        /** */
        public Person(Object name) {
            this.name = name;
        }

        /** */
        public Object name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            if (!(obj instanceof Person))
                return false;

            if (name instanceof Object[])
                return Arrays.equals((Object[])name, (Object[])((Person) obj).name);

            return F.eq(name, ((Person) obj).name);
        }
    }

    /**
     *
     */
    public enum TestEnum {
        /** */
        A,

        /** */
        B
    }
}
