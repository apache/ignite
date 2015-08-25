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

import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.portable.*;
import org.apache.ignite.portable.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

/**
 * Wildcards test.
 */
public class GridPortableWildcardsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final PortableMetaDataHandler META_HND = new PortableMetaDataHandler() {
        @Override public void addMeta(int typeId, PortableMetadata meta) {
            // No-op.
        }

        @Override public PortableMetadata metadata(int typeId) {
            return null;
        }
    };

    /**
     * @return Portable context.
     */
    private PortableContext portableContext() {
        return new PortableContext(META_HND, null);
    }

    /**
     * @return Portable marshaller.
     */
    private PortableMarshaller portableMarshaller() {
        PortableMarshaller marsh = new PortableMarshaller();
        marsh.setContext(new MarshallerContextTestImpl(null));

        return marsh;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNames() throws Exception {
        PortableContext ctx = portableContext();

        PortableMarshaller marsh = portableMarshaller();

        marsh.setClassNames(Arrays.asList(
            "org.apache.ignite.internal.portable.test.*",
            "unknown.*"
        ));

        ctx.configure(marsh);

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
        PortableContext ctx = portableContext();

        PortableMarshaller marsh = portableMarshaller();

        marsh.setIdMapper(new PortableIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 100;
                else if (clsName.endsWith("2"))
                    return 200;
                else if (clsName.endsWith("InnerClass"))
                    return 300;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        marsh.setClassNames(Arrays.asList(
            "org.apache.ignite.internal.portable.test.*",
            "unknown.*"
        ));

        ctx.configure(marsh);

        Map<String, PortableIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(100, typeMappers.get("GridPortableTestClass1").typeId("GridPortableTestClass1"));
        assertEquals(200, typeMappers.get("GridPortableTestClass2").typeId("GridPortableTestClass2"));
        assertEquals(300, typeMappers.get("InnerClass").typeId("InnerClass"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurations() throws Exception {
        PortableContext ctx = portableContext();

        PortableMarshaller marsh = portableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration("org.apache.ignite.internal.portable.test.*"),
            new PortableTypeConfiguration("unknown.*")
        ));

        ctx.configure(marsh);

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
        PortableContext ctx = portableContext();

        PortableMarshaller marsh = portableMarshaller();

        marsh.setIdMapper(new PortableIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 100;
                else if (clsName.endsWith("2"))
                    return 200;
                else if (clsName.endsWith("InnerClass"))
                    return 300;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration("org.apache.ignite.internal.portable.test.*"),
            new PortableTypeConfiguration("unknown.*")
        ));

        ctx.configure(marsh);

        Map<String, PortableIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(100, typeMappers.get("GridPortableTestClass1").typeId("GridPortableTestClass1"));
        assertEquals(200, typeMappers.get("GridPortableTestClass2").typeId("GridPortableTestClass2"));
        assertEquals(300, typeMappers.get("InnerClass").typeId("InnerClass"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsWithNonGlobalMapper() throws Exception {
        PortableContext ctx = portableContext();

        PortableMarshaller marsh = portableMarshaller();

        marsh.setIdMapper(new PortableIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 100;
                else if (clsName.endsWith("2"))
                    return 200;
                else if (clsName.endsWith("InnerClass"))
                    return 300;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration("org.apache.ignite.internal.portable.test.*"),
            new PortableTypeConfiguration("unknown.*")
        ));

        ctx.configure(marsh);

        Map<String, PortableIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(100, typeMappers.get("GridPortableTestClass1").typeId("GridPortableTestClass1"));
        assertEquals(200, typeMappers.get("GridPortableTestClass2").typeId("GridPortableTestClass2"));
        assertEquals(300, typeMappers.get("InnerClass").typeId("InnerClass"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOverride() throws Exception {
        PortableContext ctx = portableContext();

        PortableMarshaller marsh = portableMarshaller();

        marsh.setClassNames(Arrays.asList(
            "org.apache.ignite.internal.portable.test.*"
        ));

        PortableTypeConfiguration typeCfg = new PortableTypeConfiguration();

        typeCfg.setClassName("org.apache.ignite.internal.portable.test.GridPortableTestClass2");
        typeCfg.setIdMapper(new PortableIdMapper() {
            @Override public int typeId(String clsName) {
                return 100;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        marsh.setTypeConfigurations(Arrays.asList(typeCfg));

        ctx.configure(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey("gridportabletestclass1".hashCode()));
        assertTrue(typeIds.containsKey("innerclass".hashCode()));
        assertFalse(typeIds.containsKey("gridportabletestclass2".hashCode()));

        Map<String, PortableIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(100, typeMappers.get("GridPortableTestClass2").typeId("GridPortableTestClass2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesJar() throws Exception {
        PortableContext ctx = portableContext();

        PortableMarshaller marsh = portableMarshaller();

        marsh.setClassNames(Arrays.asList(
            "org.apache.ignite.portable.testjar.*",
            "unknown.*"
        ));

        ctx.configure(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey("gridportabletestclass1".hashCode()));
        assertTrue(typeIds.containsKey("gridportabletestclass2".hashCode()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassNamesWithMapperJar() throws Exception {
        PortableContext ctx = portableContext();

        PortableMarshaller marsh = portableMarshaller();

        marsh.setIdMapper(new PortableIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 100;
                else if (clsName.endsWith("2"))
                    return 200;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        marsh.setClassNames(Arrays.asList(
            "org.apache.ignite.portable.testjar.*",
            "unknown.*"
        ));

        ctx.configure(marsh);

        Map<String, PortableIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(100, typeMappers.get("GridPortableTestClass1").typeId("GridPortableTestClass1"));
        assertEquals(200, typeMappers.get("GridPortableTestClass2").typeId("GridPortableTestClass2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsJar() throws Exception {
        PortableContext ctx = portableContext();

        PortableMarshaller marsh = portableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration("org.apache.ignite.portable.testjar.*"),
            new PortableTypeConfiguration("unknown.*")
        ));

        ctx.configure(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey("gridportabletestclass1".hashCode()));
        assertTrue(typeIds.containsKey("gridportabletestclass2".hashCode()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsWithGlobalMapperJar() throws Exception {
        PortableContext ctx = portableContext();

        PortableMarshaller marsh = portableMarshaller();

        marsh.setIdMapper(new PortableIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 100;
                else if (clsName.endsWith("2"))
                    return 200;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration("org.apache.ignite.portable.testjar.*"),
            new PortableTypeConfiguration("unknown.*")
        ));

        ctx.configure(marsh);

        Map<String, PortableIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(100, typeMappers.get("GridPortableTestClass1").typeId("GridPortableTestClass1"));
        assertEquals(200, typeMappers.get("GridPortableTestClass2").typeId("GridPortableTestClass2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeConfigurationsWithNonGlobalMapperJar() throws Exception {
        PortableContext ctx = portableContext();

        PortableMarshaller marsh = portableMarshaller();

        marsh.setIdMapper(new PortableIdMapper() {
            @SuppressWarnings("IfMayBeConditional")
            @Override public int typeId(String clsName) {
                if (clsName.endsWith("1"))
                    return 100;
                else if (clsName.endsWith("2"))
                    return 200;
                else
                    return -500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration("org.apache.ignite.portable.testjar.*"),
            new PortableTypeConfiguration("unknown.*")
        ));

        ctx.configure(marsh);

        Map<String, PortableIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(100, typeMappers.get("GridPortableTestClass1").typeId("GridPortableTestClass1"));
        assertEquals(200, typeMappers.get("GridPortableTestClass2").typeId("GridPortableTestClass2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOverrideJar() throws Exception {
        PortableContext ctx = portableContext();

        PortableMarshaller marsh = portableMarshaller();

        marsh.setClassNames(Arrays.asList(
            "org.apache.ignite.portable.testjar.*"
        ));

        PortableTypeConfiguration typeCfg = new PortableTypeConfiguration(
            "org.apache.ignite.portable.testjar.GridPortableTestClass2");

        typeCfg.setIdMapper(new PortableIdMapper() {
            @Override public int typeId(String clsName) {
                return 100;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        marsh.setTypeConfigurations(Arrays.asList(typeCfg));

        ctx.configure(marsh);

        Map<Integer, Class> typeIds = U.field(ctx, "userTypes");

        assertEquals(3, typeIds.size());

        assertTrue(typeIds.containsKey("gridportabletestclass1".hashCode()));

        Map<String, PortableIdMapper> typeMappers = U.field(ctx, "typeMappers");

        assertEquals(3, typeMappers.size());

        assertEquals(100, typeMappers.get("GridPortableTestClass2").typeId("GridPortableTestClass2"));
    }
}
