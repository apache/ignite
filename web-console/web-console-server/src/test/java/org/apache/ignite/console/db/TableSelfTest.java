/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.db;

import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.console.dto.AbstractDto;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for dynamic schema changes.
 */
public class TableSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUniqueOfRecord() throws Exception {
        try (Ignite ignite = startGrid()) {
            Table<TestObject> objTbl = new Table<TestObject>(ignite, "testObjects")
                .addUniqueIndex(TestObject::getIndex, (obj) -> "Object with index '" + obj.getIndex() + "' already registered");

            TestObject obj1 = new TestObject("1");
            TestObject obj2  = new TestObject("2");

            objTbl.save(obj1);
            objTbl.save(obj2);

            GridTestUtils.assertThrows(log, () -> {
                obj1.setIndex("2");

                objTbl.save(obj1);

                return null;
            }, IgniteException.class, "Object with index '2' already registered");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateOfUniqueIndex() throws Exception {
        try (Ignite ignite = startGrid()) {
            Table<TestObject> objTbl = new Table<TestObject>(ignite, "testObjects")
                .addUniqueIndex(TestObject::getIndex, (obj) -> "Object with index '" + obj.getIndex() + "' already registered");

            TestObject obj1 = new TestObject("1");

            objTbl.save(obj1);

            assertFalse("Unique index should be created", objTbl.loadAllByIndex(Collections.singleton("1")).isEmpty());

            objTbl.save(obj1);

            assertFalse("Unique index should exists", objTbl.loadAllByIndex(Collections.singleton("1")).isEmpty());

            obj1.setIndex("2");

            objTbl.save(obj1);

            assertTrue("Unique index should be updated with record", objTbl.loadAllByIndex(Collections.singleton("1")).isEmpty());
            assertFalse("Unique index should be created", objTbl.loadAllByIndex(Collections.singleton("2")).isEmpty());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTtleOfRecord() throws Exception {
        try (Ignite ignite = startGrid()) {
            Table<TestObject> objTbl = new Table<>(
                ignite,
                "testObjects",
                1000
            );

            TestObject obj1 = new TestObject("1");

            objTbl.save(obj1);

            assertTrue("Object with index '1' exist", objTbl.containsKey(obj1.getId()));

            Thread.sleep(1000L);
            
            assertFalse("Object with index '1' expire", objTbl.containsKey(obj1.getId()));
        }
    }

    /**
     * Test object
     */
    private static class TestObject extends AbstractDto {
        /** Index. */
        private String idx;

        /**
         * @param idx Index.
         */
        TestObject(String idx) {
            super(UUID.randomUUID());

            this.idx = idx;
        }

        /**
         * @return value of index
         */
        public String getIndex() {
            return idx;
        }

        /**
         * @param idx Index.
         */
        public void setIndex(String idx) {
            this.idx = idx;
        }
    }
}
