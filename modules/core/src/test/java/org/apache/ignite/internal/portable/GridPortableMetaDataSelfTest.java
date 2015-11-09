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

package org.apache.ignite.internal.portable;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Portable meta data test.
 */
public class GridPortableMetaDataSelfTest extends GridCommonAbstractTest {
    /** */
    private static int idx;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setClassNames(Arrays.asList(TestObject1.class.getName(), TestObject2.class.getName()));

        cfg.setMarshaller(marsh);

        CacheConfiguration ccfg = new CacheConfiguration();

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        idx = 0;

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid();
    }

    /**
     * @return Portables API.
     */
    protected IgniteBinary portables() {
        return grid().binary();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAll() throws Exception {
        portables().toBinary(new TestObject2());

        Collection<BinaryType> metas = portables().metadata();

        assertEquals(2, metas.size());

        for (BinaryType meta : metas) {
            Collection<String> fields;

            switch (meta.typeName()) {
                case "TestObject1":
                    fields = meta.fields();

                    assertEquals(7, fields.size());

                    assertTrue(fields.contains("intVal"));
                    assertTrue(fields.contains("strVal"));
                    assertTrue(fields.contains("arrVal"));
                    assertTrue(fields.contains("obj1Val"));
                    assertTrue(fields.contains("obj2Val"));
                    assertTrue(fields.contains("decVal"));
                    assertTrue(fields.contains("decArrVal"));

                    assertEquals("int", meta.fieldTypeName("intVal"));
                    assertEquals("String", meta.fieldTypeName("strVal"));
                    assertEquals("byte[]", meta.fieldTypeName("arrVal"));
                    assertEquals("Object", meta.fieldTypeName("obj1Val"));
                    assertEquals("Object", meta.fieldTypeName("obj2Val"));
                    assertEquals("decimal", meta.fieldTypeName("decVal"));
                    assertEquals("decimal[]", meta.fieldTypeName("decArrVal"));

                    break;

                case "TestObject2":
                    fields = meta.fields();

                    assertEquals(7, fields.size());

                    assertTrue(fields.contains("boolVal"));
                    assertTrue(fields.contains("dateVal"));
                    assertTrue(fields.contains("uuidArrVal"));
                    assertTrue(fields.contains("objVal"));
                    assertTrue(fields.contains("mapVal"));
                    assertTrue(fields.contains("decVal"));
                    assertTrue(fields.contains("decArrVal"));

                    assertEquals("boolean", meta.fieldTypeName("boolVal"));
                    assertEquals("Date", meta.fieldTypeName("dateVal"));
                    assertEquals("UUID[]", meta.fieldTypeName("uuidArrVal"));
                    assertEquals("Object", meta.fieldTypeName("objVal"));
                    assertEquals("Map", meta.fieldTypeName("mapVal"));
                    assertEquals("decimal", meta.fieldTypeName("decVal"));
                    assertEquals("decimal[]", meta.fieldTypeName("decArrVal"));

                    break;

                default:
                    assert false : meta.typeName();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoConfiguration() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-1377");

        portables().toBinary(new TestObject3());

        assertNotNull(portables().metadata(TestObject3.class));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReflection() throws Exception {
        BinaryType meta = portables().metadata(TestObject1.class);

        assertNotNull(meta);

        assertEquals("TestObject1", meta.typeName());

        Collection<String> fields = meta.fields();

        assertEquals(7, fields.size());

        assertTrue(fields.contains("intVal"));
        assertTrue(fields.contains("strVal"));
        assertTrue(fields.contains("arrVal"));
        assertTrue(fields.contains("obj1Val"));
        assertTrue(fields.contains("obj2Val"));
        assertTrue(fields.contains("decVal"));
        assertTrue(fields.contains("decArrVal"));

        assertEquals("int", meta.fieldTypeName("intVal"));
        assertEquals("String", meta.fieldTypeName("strVal"));
        assertEquals("byte[]", meta.fieldTypeName("arrVal"));
        assertEquals("Object", meta.fieldTypeName("obj1Val"));
        assertEquals("Object", meta.fieldTypeName("obj2Val"));
        assertEquals("decimal", meta.fieldTypeName("decVal"));
        assertEquals("decimal[]", meta.fieldTypeName("decArrVal"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableMarshalAware() throws Exception {
        portables().toBinary(new TestObject2());

        BinaryType meta = portables().metadata(TestObject2.class);

        assertNotNull(meta);

        assertEquals("TestObject2", meta.typeName());

        Collection<String> fields = meta.fields();

        assertEquals(7, fields.size());

        assertTrue(fields.contains("boolVal"));
        assertTrue(fields.contains("dateVal"));
        assertTrue(fields.contains("uuidArrVal"));
        assertTrue(fields.contains("objVal"));
        assertTrue(fields.contains("mapVal"));
        assertTrue(fields.contains("decVal"));
        assertTrue(fields.contains("decArrVal"));

        assertEquals("boolean", meta.fieldTypeName("boolVal"));
        assertEquals("Date", meta.fieldTypeName("dateVal"));
        assertEquals("UUID[]", meta.fieldTypeName("uuidArrVal"));
        assertEquals("Object", meta.fieldTypeName("objVal"));
        assertEquals("Map", meta.fieldTypeName("mapVal"));
        assertEquals("decimal", meta.fieldTypeName("decVal"));
        assertEquals("decimal[]", meta.fieldTypeName("decArrVal"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMerge() throws Exception {
        portables().toBinary(new TestObject2());

        idx = 1;

        portables().toBinary(new TestObject2());

        BinaryType meta = portables().metadata(TestObject2.class);

        assertNotNull(meta);

        assertEquals("TestObject2", meta.typeName());

        Collection<String> fields = meta.fields();

        assertEquals(9, fields.size());

        assertTrue(fields.contains("boolVal"));
        assertTrue(fields.contains("dateVal"));
        assertTrue(fields.contains("uuidArrVal"));
        assertTrue(fields.contains("objVal"));
        assertTrue(fields.contains("mapVal"));
        assertTrue(fields.contains("charVal"));
        assertTrue(fields.contains("colVal"));
        assertTrue(fields.contains("decVal"));
        assertTrue(fields.contains("decArrVal"));

        assertEquals("boolean", meta.fieldTypeName("boolVal"));
        assertEquals("Date", meta.fieldTypeName("dateVal"));
        assertEquals("UUID[]", meta.fieldTypeName("uuidArrVal"));
        assertEquals("Object", meta.fieldTypeName("objVal"));
        assertEquals("Map", meta.fieldTypeName("mapVal"));
        assertEquals("char", meta.fieldTypeName("charVal"));
        assertEquals("Collection", meta.fieldTypeName("colVal"));
        assertEquals("decimal", meta.fieldTypeName("decVal"));
        assertEquals("decimal[]", meta.fieldTypeName("decArrVal"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerializedObject() throws Exception {
        TestObject1 obj = new TestObject1();

        obj.intVal = 10;
        obj.strVal = "str";
        obj.arrVal = new byte[] {2, 4, 6};
        obj.obj1Val = null;
        obj.obj2Val = new TestObject2();
        obj.decVal = BigDecimal.ZERO;
        obj.decArrVal = new BigDecimal[] { BigDecimal.ONE };

        BinaryObject po = portables().toBinary(obj);

        info(po.toString());

        BinaryType meta = po.type();

        assertNotNull(meta);

        assertEquals("TestObject1", meta.typeName());

        Collection<String> fields = meta.fields();

        assertEquals(7, fields.size());

        assertTrue(fields.contains("intVal"));
        assertTrue(fields.contains("strVal"));
        assertTrue(fields.contains("arrVal"));
        assertTrue(fields.contains("obj1Val"));
        assertTrue(fields.contains("obj2Val"));
        assertTrue(fields.contains("decVal"));
        assertTrue(fields.contains("decArrVal"));

        assertEquals("int", meta.fieldTypeName("intVal"));
        assertEquals("String", meta.fieldTypeName("strVal"));
        assertEquals("byte[]", meta.fieldTypeName("arrVal"));
        assertEquals("Object", meta.fieldTypeName("obj1Val"));
        assertEquals("Object", meta.fieldTypeName("obj2Val"));
        assertEquals("decimal", meta.fieldTypeName("decVal"));
        assertEquals("decimal[]", meta.fieldTypeName("decArrVal"));
    }

    /**
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class TestObject1 {
        /** */
        private int intVal;

        /** */
        private String strVal;

        /** */
        private byte[] arrVal;

        /** */
        private TestObject1 obj1Val;

        /** */
        private TestObject2 obj2Val;

        /** */
        private BigDecimal decVal;

        /** */
        private BigDecimal[] decArrVal;
    }

    /**
     */
    private static class TestObject2 implements Binarylizable {
        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeBoolean("boolVal", false);
            writer.writeDate("dateVal", new Date());
            writer.writeUuidArray("uuidArrVal", null);
            writer.writeObject("objVal", null);
            writer.writeMap("mapVal", new HashMap<>());
            writer.writeDecimal("decVal", BigDecimal.ZERO);
            writer.writeDecimalArray("decArrVal", new BigDecimal[] { BigDecimal.ONE });

            if (idx == 1) {
                writer.writeChar("charVal", (char)0);
                writer.writeCollection("colVal", null);
            }

            BinaryRawWriter raw = writer.rawWriter();

            raw.writeChar((char)0);
            raw.writeCollection(null);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            // No-op.
        }
    }

    /**
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class TestObject3 {
        /** */
        private int intVal;
    }
}