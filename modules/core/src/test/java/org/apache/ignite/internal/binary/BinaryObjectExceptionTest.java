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

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * BinaryObjectExceptionTest
 */
public class BinaryObjectExceptionTest extends GridCommonAbstractTest {

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new BinaryMarshaller());
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setCopyOnRead(true);
        cfg.setCacheConfiguration(ccfg);

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

    /** */
    public void testUnexpectedFieldType() throws Exception {
        IgniteEx grid = grid();
        IgniteCache<String, Value> cache = grid.cache(null);
        String n = "key";
        cache.put(n, new Value());
        BinaryObjectImpl b = (BinaryObjectImpl)cache.withKeepBinary().get(n);
        b.deserialize(); // deserialize working
        byte[] a = b.array();
        int unexpectedCnt = 0;
        Field[] fields = Value.class.getDeclaredFields();
        for (int i = b.dataStartOffset(), j = b.footerStartOffset(); i < j; ++i) {
            byte old = a[i];
            a[i] = -1;
            try {
                b.deserialize(); // deserialize error
            }
            catch (BinaryObjectException ex) {
                Throwable root = ex;
                StringBuilder sb = new StringBuilder(4 << 10);
                sb.append(root.getMessage());
                while (root.getCause() != null) {
                    root = root.getCause();
                    sb.append(". ").append(root.getMessage());
                }
                if (root instanceof BinaryObjectException &&
                    root.getMessage().startsWith("Unexpected field type")) {
                    log().info(sb.toString());
                    Field f = fields[unexpectedCnt];
                    assertTrue("type must be equal \"org.apache.ignite.internal.binary.BinaryObjectExceptionTest$Value\"",
                        ex.getMessage().
                            contains("type: org.apache.ignite.internal.binary.BinaryObjectExceptionTest$Value"));
                    assertTrue("field must be equal \"" + f.getName() + "\"",
                        ex.getCause().getMessage().
                            contains("field: " + f.getName()));
                    ++unexpectedCnt;
                }
            }
            catch (Exception ignore) {
            }
            a[i] = old;
        }
        assertEquals("Fields count must match \"Unexpected field type\" exception count",
            fields.length, unexpectedCnt);
    }

    /** */
    private enum EnumValues {

        /** */
        val1,

        /** */
        val2,

        /** */
        val3;
    }

    /** */
    private static class Value {

        /** */
        public byte byteVal = 1;

        /** */
        public boolean booleanVal = true;

        /** */
        public short shortVal = 2;

        /** */
        public char charVal = 'Q';

        /** */
        public int intVal = 3;

        /** */
        public long longVal = 4;

        /** */
        public float floatVal = 5;

        /** */
        public double doubleVal = 6;

        /** */
        public BigDecimal bigDecimal = new BigDecimal(7);

        /** */
        public String string = "QWERTY";

        /** */
        public UUID uuid = UUID.randomUUID();

        /** */
        public Date date = new Date();

        /** */
        public Timestamp timestamp = new Timestamp(date.getTime() + 1000L);

        /** */
        public EnumValues enumVal = EnumValues.val2;
    }
}
