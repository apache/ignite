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

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.marshaller.portable.BinaryMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Arrays;
import java.util.Map;

/**
 * Wildcards test.
 */
public class GridPortableWildcardsSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testClassNames() throws Exception {
        BinaryMarshaller marsh = portableMarshaller(Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.portable.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        PortableContext ctx = portableContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey("gridportabletestclass1".hashCode()));
        assertTrue(typeIds.containsKey("gridportabletestclass2".hashCode()));
        assertTrue(typeIds.containsKey("innerclass".hashCode()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesWithMapper() throws Exception {
        BinaryMarshaller marsh = portableMarshaller(new BinaryIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 300;
                else if (clsName.endsWith("2"))
                    return 400;
                else if (clsName.endsWith("InnerClass"))
                    return 500;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        }, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.portable.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        PortableContext ctx = portableContext(marsh);

        Map<String, BinaryIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(300, typeMappers.get("GridPortableTestClass1").typeId("GridPortableTestClass1"));
        assertEquals(400, typeMappers.get("GridPortableTestClass2").typeId("GridPortableTestClass2"));
        assertEquals(500, typeMappers.get("InnerClass").typeId("InnerClass"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurations() throws Exception {
        BinaryMarshaller marsh = portableMarshaller(Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.portable.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        PortableContext ctx = portableContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey("gridportabletestclass1".hashCode()));
        assertTrue(typeIds.containsKey("gridportabletestclass2".hashCode()));
        assertTrue(typeIds.containsKey("innerclass".hashCode()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsWithGlobalMapper() throws Exception {
        BinaryMarshaller marsh = portableMarshaller(new BinaryIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 300;
                else if (clsName.endsWith("2"))
                    return 400;
                else if (clsName.endsWith("InnerClass"))
                    return 500;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        }, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.portable.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        PortableContext ctx = portableContext(marsh);

        Map<String, BinaryIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(300, typeMappers.get("GridPortableTestClass1").typeId("GridPortableTestClass1"));
        assertEquals(400, typeMappers.get("GridPortableTestClass2").typeId("GridPortableTestClass2"));
        assertEquals(500, typeMappers.get("InnerClass").typeId("InnerClass"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsWithNonGlobalMapper() throws Exception {
        BinaryMarshaller marsh = portableMarshaller(new BinaryIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 300;
                else if (clsName.endsWith("2"))
                    return 400;
                else if (clsName.endsWith("InnerClass"))
                    return 500;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        }, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.portable.test.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        PortableContext ctx = portableContext(marsh);

        Map<String, BinaryIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(300, typeMappers.get("GridPortableTestClass1").typeId("GridPortableTestClass1"));
        assertEquals(400, typeMappers.get("GridPortableTestClass2").typeId("GridPortableTestClass2"));
        assertEquals(500, typeMappers.get("InnerClass").typeId("InnerClass"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOverride() throws Exception {
        BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration();

        typeCfg.setTypeName("org.apache.ignite.internal.portable.test.GridPortableTestClass2");
        typeCfg.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 100;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryMarshaller marsh = portableMarshaller(Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.internal.portable.test.*"),
            typeCfg));

        PortableContext ctx = portableContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey("gridportabletestclass1".hashCode()));
        assertTrue(typeIds.containsKey("innerclass".hashCode()));
        assertFalse(typeIds.containsKey("gridportabletestclass2".hashCode()));

        Map<String, BinaryIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(100, typeMappers.get("GridPortableTestClass2").typeId("GridPortableTestClass2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesJar() throws Exception {
        BinaryMarshaller marsh = portableMarshaller(Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.portable.testjar.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        PortableContext ctx = portableContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey("gridportabletestclass1".hashCode()));
        assertTrue(typeIds.containsKey("gridportabletestclass2".hashCode()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesWithMapperJar() throws Exception {
        BinaryMarshaller marsh = portableMarshaller(new BinaryIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 300;
                else if (clsName.endsWith("2"))
                    return 400;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        }, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.portable.testjar.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        PortableContext ctx = portableContext(marsh);

        Map<String, BinaryIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(300, typeMappers.get("GridPortableTestClass1").typeId("GridPortableTestClass1"));
        assertEquals(400, typeMappers.get("GridPortableTestClass2").typeId("GridPortableTestClass2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsJar() throws Exception {
        BinaryMarshaller marsh = portableMarshaller(Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.portable.testjar.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        PortableContext ctx = portableContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey("gridportabletestclass1".hashCode()));
        assertTrue(typeIds.containsKey("gridportabletestclass2".hashCode()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsWithGlobalMapperJar() throws Exception {
        BinaryMarshaller marsh = portableMarshaller(new BinaryIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 300;
                else if (clsName.endsWith("2"))
                    return 400;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        }, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.portable.testjar.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        PortableContext ctx = portableContext(marsh);

        Map<String, BinaryIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(300, typeMappers.get("GridPortableTestClass1").typeId("GridPortableTestClass1"));
        assertEquals(400, typeMappers.get("GridPortableTestClass2").typeId("GridPortableTestClass2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsWithNonGlobalMapperJar() throws Exception {
        BinaryMarshaller marsh = portableMarshaller(new BinaryIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 300;
                else if (clsName.endsWith("2"))
                    return 400;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        }, Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.portable.testjar.*"),
            new BinaryTypeConfiguration("unknown.*")
        ));

        PortableContext ctx = portableContext(marsh);

        Map<String, BinaryIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(300, typeMappers.get("GridPortableTestClass1").typeId("GridPortableTestClass1"));
        assertEquals(400, typeMappers.get("GridPortableTestClass2").typeId("GridPortableTestClass2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOverrideJar() throws Exception {
        BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration(
            "org.apache.ignite.portable.testjar.GridPortableTestClass2");

        typeCfg.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 100;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryMarshaller marsh = portableMarshaller(Arrays.asList(
            new BinaryTypeConfiguration("org.apache.ignite.portable.testjar.*"),
            typeCfg));

        PortableContext ctx = portableContext(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey("gridportabletestclass1".hashCode()));

        Map<String, BinaryIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(100, typeMappers.get("GridPortableTestClass2").typeId("GridPortableTestClass2"));
    }

    /**
     * @param marsh Marshaller.
     * @return Portable context.
     */
    protected PortableContext portableContext(BinaryMarshaller marsh) {
        GridPortableMarshaller impl = U.field(marsh, "impl");

        return impl.context();
    }

    /**
     *
     */
    protected BinaryMarshaller portableMarshaller()
        throws IgniteCheckedException {
        return portableMarshaller(null, null, null);
    }

    /**
     *
     */
    protected BinaryMarshaller portableMarshaller(Collection<BinaryTypeConfiguration> cfgs)
        throws IgniteCheckedException {
        return portableMarshaller(null, null, cfgs);
    }

    /**
     *
     */
    protected BinaryMarshaller portableMarshaller(BinaryIdMapper mapper, Collection<BinaryTypeConfiguration> cfgs)
        throws IgniteCheckedException {
        return portableMarshaller(mapper, null, cfgs);
    }

    /**
     *
     */
    protected BinaryMarshaller portableMarshaller(BinarySerializer serializer, Collection<BinaryTypeConfiguration> cfgs)
        throws IgniteCheckedException {
        return portableMarshaller(null, serializer, cfgs);
    }

    protected BinaryMarshaller portableMarshaller(
        BinaryIdMapper mapper,
        BinarySerializer serializer,
        Collection<BinaryTypeConfiguration> cfgs
    ) throws IgniteCheckedException {
        IgniteConfiguration iCfg = new IgniteConfiguration();

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setIdMapper(mapper);
        bCfg.setSerializer(serializer);

        bCfg.setTypeConfigurations(cfgs);

        iCfg.setBinaryConfiguration(bCfg);

        PortableContext ctx = new PortableContext(BinaryNoopMetadataHandler.instance(), iCfg);

        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(new MarshallerContextTestImpl(null));

        IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setPortableContext", ctx, iCfg);

        return marsh;
    }
}