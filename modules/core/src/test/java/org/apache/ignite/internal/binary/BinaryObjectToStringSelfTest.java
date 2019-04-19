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

package org.apache.ignite.internal.binary;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@code BinaryObject.toString()}.
 */
@RunWith(JUnit4.class)
public class BinaryObjectToStringSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testToString() throws Exception {
        MyObject obj = new MyObject();

        obj.arr = new Object[] {111, "aaa", obj};
        obj.col = Arrays.asList(222, "bbb", obj);

        obj.map = new HashMap();

        obj.map.put(10, 333);
        obj.map.put(20, "ccc");
        obj.map.put(30, obj);

        BinaryObject bo = grid().binary().toBinary(obj);

        // Check that toString() doesn't fail with StackOverflowError or other exceptions.
        bo.toString();
    }

    /**
     */
    private static class MyObject {
        /** Object array. */
        private Object[] arr;

        /** Collection. */
        private Collection col;

        /** Map. */
        private Map map;
    }
}
