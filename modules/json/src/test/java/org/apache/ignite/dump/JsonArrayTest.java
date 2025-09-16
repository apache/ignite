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

package org.apache.ignite.dump;

import java.util.Map;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.jackson.IgniteObjectMapper;
import org.apache.ignite.platform.model.Department;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_USE_BINARY_ARRAYS;
import static org.apache.ignite.internal.binary.BinaryUtils.DFLT_IGNITE_USE_BINARY_ARRAYS;

/** */
@RunWith(Parameterized.class)
public class JsonArrayTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter
    public boolean useBinaryArrays;

    /**
     * @return Test parameters.
     */
    @Parameterized.Parameters(name = "useBinaryArrays={0}")
    public static Object[] parameters() {
        return new Object[] {true, false};
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        System.setProperty(IGNITE_USE_BINARY_ARRAYS, useBinaryArrays + "");
        BinaryUtils.initUseBinaryArrays();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        System.setProperty(IGNITE_USE_BINARY_ARRAYS, DFLT_IGNITE_USE_BINARY_ARRAYS + "");
        BinaryUtils.initUseBinaryArrays();
    }

    /** */
    @Test
    public void testArrayFieldToBinary() throws Exception {
        try (IgniteEx srv = startGrid(0)) {
            IgniteObjectMapper mapper = new IgniteObjectMapper(srv.context());

            Strings strs = new Strings();

            strs.setValues(new String[]{"John Connor", "Sarah Connor", "Kyle Reese"});

            BinaryObject binary = srv.binary().toBinary(strs);

            String json = mapper.writeValueAsString(binary);

            assertNotNull(json);

            Strings fromJson = mapper.readValue(json, new TypeReference<>() {});

            assertNotNull(fromJson);
            assertEquals(strs.values.length, fromJson.values.length);

            for (int i = 0; i < strs.values.length; i++)
                assertEquals(strs.values[i], fromJson.values[i]);
        }
    }

    /** */
    @Test
    public void testCustomClassArrayToBinary() throws Exception {
        try (IgniteEx srv = startGrid(0)) {
            IgniteObjectMapper mapper = new IgniteObjectMapper(srv.context());

            User[] raw = new User[]{
                new User("John Connor", 10.0d, new Department("IT")),
                new User("Sarah Connor", 20.0d, new Department("SEC")),
                new User("Kyle Reese", 50.0d, new Department("SEC")),
            };

            Object binary = srv.binary().toBinary(raw);

            assertEquals(useBinaryArrays, BinaryUtils.isBinaryArray(binary));

            String json = mapper.writeValueAsString(binary);

            assertNotNull(json);

            Object[] fromJson = mapper.readValue(json, new TypeReference<>() {});

            assertNotNull(fromJson);
            assertEquals(raw.length, fromJson.length);

            for (int i = 0; i < raw.length; i++) {
                User user = raw[i];
                Map<String, Object> data = (Map<String, Object>)fromJson[i];

                assertEquals(user.name, data.get("name"));
                assertEquals(user.salary, data.get("salary"));
                assertEquals(user.dep.getName(), ((Map)data.get("dep")).get("name"));
            }
        }
    }

    /** */
    public static class User {
        /** */
        private String name;

        /** */
        private Double salary;

        /** */
        private Department dep;

        /** */
        public User(String name, Double salary, Department dep) {
            this.name = name;
            this.salary = salary;
            this.dep = dep;
        }

        /** */
        public String getName() {
            return name;
        }

        /** */
        public void setName(String name) {
            this.name = name;
        }

        /** */
        public Double getSalary() {
            return salary;
        }

        /** */
        public void setSalary(Double salary) {
            this.salary = salary;
        }

        /** */
        public Department getDep() {
            return dep;
        }

        /** */
        public void setDep(Department dep) {
            this.dep = dep;
        }
    }

    /** */
    public static class Strings {
        /** */
        public String[] values;

        /** */
        public String[] getValues() {
            return values;
        }

        /** */
        public void setValues(String[] values) {
            this.values = values;
        }
    }
}
