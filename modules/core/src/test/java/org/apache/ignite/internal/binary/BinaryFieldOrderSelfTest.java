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

package org.apache.ignite.internal.binary;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test that field ordering doesn't change the schema.
 */
public class BinaryFieldOrderSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new BinaryMarshaller());
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder(true)));

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
    public void testEquals() throws Exception {
        IgniteEx ignite = grid();

        BinaryObject bo0 = ignite.binary().toBinary(new MyType(222, 333, 111));

        BinaryObject bo1 = ignite.binary().builder(bo0.type().typeName()).
            setField("b", 222).
            setField("c", 333).
            setField("a", 111).
            hashCode(12345).
            build();

        BinaryObject bo2 = ignite.binary().builder(bo0.type().typeName()).
            setField("a", 111).
            setField("b", 222).
            setField("c", 333).
            hashCode(12345).
            build();

        assertEquals(12345, bo0.hashCode());
        assertEquals(12345, bo1.hashCode());
        assertEquals(12345, bo2.hashCode());

        assertTrue(bo0.equals(bo1));
        assertTrue(bo0.equals(bo2));
        assertTrue(bo1.equals(bo2));
    }

    /**
     */
    private static class MyType {
        /** B. */
        private int b;

        /** C. */
        private int c;

        /** A. */
        private int a;

        /**
         * @param b B.
         * @param c C.
         * @param a A.
         */
        MyType(int b, int c, int a) {
            this.b = b;
            this.c = c;
            this.a = a;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return super.equals(obj);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 12345;
        }
    }
}
