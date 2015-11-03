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

import java.util.Arrays;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for disabled meta data.
 */
public class GridPortableMetaDataDisabledSelfTest extends GridCommonAbstractTest {
    /** */
    private PortableMarshaller marsh;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(marsh);

        return cfg;
    }

    /**
     * @return Portables.
     */
    private IgniteBinary portables() {
        return grid().binary();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisableGlobal() throws Exception {
        marsh = new PortableMarshaller();

        marsh.setClassNames(Arrays.asList(
            TestObject1.class.getName(),
            TestObject2.class.getName()
        ));

        marsh.setMetaDataEnabled(false);

        try {
            startGrid();

            portables().toBinary(new TestObject1());
            portables().toBinary(new TestObject2());
            portables().toBinary(new TestObject3());

            assertEquals(0, portables().metadata(TestObject1.class).fields().size());
            assertEquals(0, portables().metadata(TestObject2.class).fields().size());

            BinaryObjectBuilder bldr = portables().builder("FakeType");

            bldr.setField("field1", 0).setField("field2", "value").build();

            assertNull(portables().metadata("FakeType"));
            assertNull(portables().metadata(TestObject3.class));
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisableGlobalSimpleClass() throws Exception {
        marsh = new PortableMarshaller();

        BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration(TestObject2.class.getName());

        typeCfg.setMetaDataEnabled(true);

        marsh.setTypeConfigurations(Arrays.asList(
            new BinaryTypeConfiguration(TestObject1.class.getName()),
            typeCfg
        ));

        marsh.setMetaDataEnabled(false);

        try {
            startGrid();

            portables().toBinary(new TestObject1());
            portables().toBinary(new TestObject2());

            assertEquals(0, portables().metadata(TestObject1.class).fields().size());
            assertEquals(1, portables().metadata(TestObject2.class).fields().size());
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisableGlobalMarshalAwareClass() throws Exception {
        marsh = new PortableMarshaller();

        BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration(TestObject1.class.getName());

        typeCfg.setMetaDataEnabled(true);

        marsh.setTypeConfigurations(Arrays.asList(
            new BinaryTypeConfiguration(TestObject2.class.getName()),
            typeCfg
        ));

        marsh.setMetaDataEnabled(false);

        try {
            startGrid();

            portables().toBinary(new TestObject1());
            portables().toBinary(new TestObject2());

            assertEquals(1, portables().metadata(TestObject1.class).fields().size());
            assertEquals(0, portables().metadata(TestObject2.class).fields().size());
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisableSimpleClass() throws Exception {
        marsh = new PortableMarshaller();

        BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration(TestObject1.class.getName());

        typeCfg.setMetaDataEnabled(false);

        marsh.setTypeConfigurations(Arrays.asList(
            new BinaryTypeConfiguration(TestObject2.class.getName()),
            typeCfg
        ));

        try {
            startGrid();

            portables().toBinary(new TestObject1());
            portables().toBinary(new TestObject2());

            assertEquals(0, portables().metadata(TestObject1.class).fields().size());
            assertEquals(1, portables().metadata(TestObject2.class).fields().size());
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDisableMarshalAwareClass() throws Exception {
        marsh = new PortableMarshaller();

        BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration(TestObject2.class.getName());

        typeCfg.setMetaDataEnabled(false);

        marsh.setTypeConfigurations(Arrays.asList(
            new BinaryTypeConfiguration(TestObject1.class.getName()),
            typeCfg
        ));

        try {
            startGrid();

            portables().toBinary(new TestObject1());
            portables().toBinary(new TestObject2());

            assertEquals(1, portables().metadata(TestObject1.class).fields().size());
            assertEquals(0, portables().metadata(TestObject2.class).fields().size());
        }
        finally {
            stopGrid();
        }
    }

    /**
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class TestObject1 {
        /** */
        private int field;
    }

    /**
     */
    private static class TestObject2 implements Binarylizable {
        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeInt("field", 1);
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
        private int field;
    }
}